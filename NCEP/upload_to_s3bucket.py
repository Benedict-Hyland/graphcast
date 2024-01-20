from run_graphcast import GraphCastModel

def upload_to_s3_wrapper(input_path, output_path, upload, keep):
    runner = GraphCastModel()
    runner.upload_to_s3(input_path, output_path, keep)

if __name__ == "__main__":
    # Specify the arguments directly or use argparse if needed
    input_path = "/path/to/input/file"
    output_path = "/path/to/output/file"
    upload = "yes"
    keep = "yes"
  
