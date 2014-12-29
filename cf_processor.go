package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path"
	"strings"
	//"sync"
	"io"

	"github.com/rackspace/gophercloud"
	"github.com/rackspace/gophercloud/rackspace"
	// "github.com/rackspace/gophercloud/rackspace/objectstorage/v1/cdncontainers"
	//"github.com/rackspace/gophercloud/openstack/objectstorage/v1/containers"
	// "github.com/rackspace/gophercloud/rackspace/objectstorage/v1/objects"
	"github.com/rackspace/gophercloud/openstack/objectstorage/v1/objects"
)

type ExecCmd struct {
	ExecuteCommand  exec.Cmd
	LocalOutputPath string
	ContentType     string
	CFContainerOut  string
}

func init() {
	// Nothing to do yet
}

func printHeader(headers http.Header) {
	fmt.Println("Container Header Info: ")
	for k, v := range headers {
		fmt.Println(k, v)
	}
}

func errorHandler(err error) {
	if err != nil {
		panic(err)
	}
}

func setupCmd(command_string string, stdout io.Writer, stdin io.Reader, stderr io.Writer) exec.Cmd {
	cmd_list := strings.Fields(command_string)
	fmt.Printf("%s\n", cmd_list)
	cmd_exec := cmd_list[0]               // grab head of the list
	cmd_args := cmd_list[1:len(cmd_list)] // grab the args
	cmd := exec.Command(cmd_exec, cmd_args...)
	cmd.Stdout = stdout
	cmd.Stdin = stdin
	cmd.Stderr = stderr
	return *cmd
}

func uploadObjectCF(service_client *gophercloud.ServiceClient, content_type string, container_name string, object_name string, content_upload io.Reader) {
	// Setup upload stream for cmd0
	opts := objects.CreateOpts{
		ContentType: content_type,
	}

	res := objects.Create(service_client, container_name, object_name, content_upload, opts)
	headers, err := res.ExtractHeader()
	if err != nil {
		panic(err)
	}
	printHeader(headers)
}

func downloadObjectCF(service_client *gophercloud.ServiceClient, input_container string, local_path_prefix string, raw_video_name string) (string, error) {

	cf_download := objects.Download(service_client, input_container, raw_video_name, nil)

	// Create a directory named for the raw_video_name with 'tmp-' prefix
	raw_video_dir := path.Join(local_path_prefix, "tmp-"+raw_video_name)
	err := os.MkdirAll(raw_video_dir, 744)
	if err != nil {
		panic(err)
	}

	// Create file and stream contents to it to bypass copying into memory
	raw_video_path := path.Join(raw_video_dir, raw_video_name)
	raw_video_download, err := os.Create(raw_video_path)
	if err != nil {
		panic(err)
	}
	defer raw_video_download.Close()

	_, err = io.Copy(raw_video_download, cf_download.Body)
	if err != nil {
		return raw_video_path, err
	}

	headers, err := cf_download.ExtractHeader()
	if err != nil {
		panic(err)
	}
	printHeader(headers)

	return raw_video_path, err
}

func video_processing_task(execute_command exec.Cmd, local_video_path string, cf_container_output string, content_type string) *ExecCmd {

	cmd := new(ExecCmd)
	cmd.ExecuteCommand = execute_command
	cmd.LocalOutputPath = local_video_path
	cmd.CFContainerOut = cf_container_output
	cmd.ContentType = content_type
	return cmd
}

func main() {
	var output_container, input_container, raw_video_name, local_path_prefix string

	flag.StringVar(&output_container,
		"output_container",
		"video_output",
		"Cloud Files output container.")
	flag.StringVar(&input_container,
		"input_container",
		"video_input",
		"Cloud Files input container.")
	flag.StringVar(&raw_video_name,
		"raw_video",
		"movie.mov",
		"Name of the video to be processed.")
	flag.StringVar(&local_path_prefix,
		"localprefix",
		"/go/video",
		"Local directory where the video will be processed and uploaded.")
	flag.Parse()

	opts, err := rackspace.AuthOptionsFromEnv()
	if err != nil {
		panic(err)
	}

	provider, err := rackspace.AuthenticatedClient(opts)
	if err != nil {
		panic(err)
	}

	cfClient, err := rackspace.NewObjectStorageV1(provider,
		gophercloud.EndpointOpts{
			Region: "IAD",
		})

	file_path, err := downloadObjectCF(cfClient,
		input_container,
		local_path_prefix,
		raw_video_name)

	// Initialize a slice with our video processing tasks
	video_task_slice := make([]*ExecCmd, 0)

	// Get video name without the extension
	video_name_wo_ext := strings.Split(raw_video_name, ".")[0]

	// Local directory to contain all of the videos
	local_video_path_dir, _ := path.Split(file_path)

	// Setup the task to process webm video
	webm := `ffmpeg -i %s -vcodec libvpx -acodec libvorbis -pix_fmt yuv420p
					-quality good -b:v 2M -crf 5 -movflags faststart
					-vf scale=trunc(in_w/2)*2:trunc(in_h/2)*2
					-f webm -y %s`

	webm_video_path := path.Join(local_video_path_dir, video_name_wo_ext+".webm")
	webm_cmd_str := fmt.Sprintf(webm, file_path, webm_video_path)
	webm_cmd := setupCmd(webm_cmd_str, os.Stdout, nil, os.Stderr)
	video_task_slice = append(video_task_slice,
		video_processing_task(webm_cmd,
			webm_video_path,
			output_container,
			"video/webm"))

	// Setup the task to process mp4 videos
	mp4 := `ffmpeg 	-i %s -vcodec libx264 -pix_fmt yuv420p -profile:v baseline
					-preset slower -movflags faststart -strict -2 -crf 18
					-vf scale=trunc(in_w/2)*2:trunc(in_h/2)*2
					-y %s`
	mp4_video_path := path.Join(local_video_path_dir, video_name_wo_ext+".mp4")
	mp4_cmd_str := fmt.Sprintf(mp4, file_path, mp4_video_path)
	mp4_cmd := setupCmd(mp4_cmd_str, os.Stdout, nil, os.Stderr)
	video_task_slice = append(video_task_slice,
		video_processing_task(mp4_cmd,
			mp4_video_path,
			output_container,
			"video/mp4"))

	// Setup the task to process thumbnail
	jpg := `ffmpeg 	-i %s -ss 00:00:03.435 -vcodec mjpeg -q:v 10 -vf scale=200:-1
					-vframes 1 -an -f image2 -y %s`
	jpg_video_path := path.Join(local_video_path_dir, video_name_wo_ext+".jpg")
	jpg_cmd_str := fmt.Sprintf(jpg, file_path, jpg_video_path)
	jpg_cmd := setupCmd(jpg_cmd_str, os.Stdout, nil, os.Stderr)
	video_task_slice = append(video_task_slice,
		video_processing_task(jpg_cmd,
			jpg_video_path,
			output_container,
			"image/jpg"))

	//var wg sync.WaitGroup
	//wg.Add(len(video_task_slice))

	for _, cmd := range video_task_slice {
		// go func(){
		err := cmd.ExecuteCommand.Run()
		if err != nil {
			panic(err)
		}
		fd, err := os.Open(cmd.LocalOutputPath)
		if err != nil {
			panic(err)
		}

		_, filename_to_upload := path.Split(cmd.LocalOutputPath)

		uploadObjectCF(cfClient,
			cmd.ContentType,
			cmd.CFContainerOut,
			filename_to_upload,
			fd)
		fd.Close()
		//wg.Done()
	}
	//wg.Wait()
	fmt.Println("Video Processing Completed!")
}
