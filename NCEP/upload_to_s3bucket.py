from run_graphcast import GraphCastModel

def upload_to_s3_wrapper(input_path, output_path, upload, keep):
    runner = GraphCastModel()
    runner.upload_to_s3(input_path, output_path, keep)

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description="upload input and output to s3 bucket")
    parser.add_argument("-i", "--input", help="input file path (including file name)", required=True)
    parser.add_argument("-o", "--output", help="output file path (including file name)", required=True)
    args = parser.parse_args()
    keep = False

    upload_to_s3_wrapper(args.input, args.output, keep)
