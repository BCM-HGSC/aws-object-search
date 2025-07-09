#!/usr/bin/env python3
import argparse
import csv
import json
import logging
import re
import sys
from time import strftime

import boto3


"""
Search AWS Glacier bucket files based on user's inputs

Example command:
python searchGlacier.py -f HABCDED-3-IDUD001234 -b hgsc-dev
python searchGlacier.py -b hgsc-alzheimers -m BCM_ABCD_EDGFI_SIC1234_1.cram.crai
python searchGlacier.py -b hgsccl-czi -m "SIC1234.*fastq_list"
"""

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(message)s")

api_call_count = 0

def track_api_call():
    """Increment API call counter and log the call."""
    global api_call_count
    api_call_count += 1
    #logger.info(f"AWS API Call #{api_call_count}")

def parse_args():
    """ CLI interface to intake inputs
    RETURNS: args
    """
    parser = argparse.ArgumentParser(
        usage = "\nCheck how to use this script: python searchGlacier.py -h\n\nExample command: python searchGlacier.py -f HABCDED-3-IDUD001234 -b hgsc-dev \n\nExample output: {'Key': '38-clinical-dragen-pipeline/fastqs/Project_HABCDEF/Sample_HABCDEF-2-IDABCD1234/dragen/2000_A00000_000_BABCD-2-IDABCD1234.qc-coverage-region-1_cov_report.bed', 'LastModified': datetime.datetime(2024, 12, 01, 00, 00, 2, tzinfo=tzutc()), 'ETag': ''cd00000'', 'Size': 9000, 'StorageClass': 'STANDARD'}"
        )
    parser.add_argument(
        "-f",
        "--flowcell-lane-barcode",
        help = "(Optional) Search flowcell-lane-barcode, e.g. HABCDEFG-3-IDU000000"
        )
    parser.add_argument(
        "-b",
        "--bucket",
        help = "(Optional) Name of the AWS S3 bucket"
        )
    parser.add_argument(
        "-p",
        "--platform",
        help = "(Optional) Name of platform (e.g. Illumina)"
        )
    parser.add_argument(
        "-t",
        "--technology",
        help = "(Optional) Name of technology type (e.g. wholegenome)"
        )
    parser.add_argument(
        "-s",
        "--storageClass",
        help = "(Optional) Expected storage class of the files"
        )
    parser.add_argument(
        "-m",
        "--match-substring",
        help = "(Optional) Part of the path name or file name" ####
        )
    parser.add_argument(
        "-c",
        "--csv",
        help = "(Optional) Full path of the output csv"
        )

    try:
        args = parser.parse_args()
    except:
        logger.error("The input parameters are not valid")
        sys.exit()
    return args

def get_all_buckets():
    """Retrieve all available S3 bucket names."""
    try:
        s3 = boto3.client("s3")
        track_api_call()
        response = s3.list_buckets()
        return [bucket["Name"] for bucket in response["Buckets"]]
    except Exception as e:
        logger.error(f"Error retrieving bucket list: {e}")
        sys.exit()

def search_object(optional_inputs):
    """Search for an object across one or multiple S3 buckets."""
    try:
        s3 = boto3.client("s3")
        paginator = s3.get_paginator("list_objects_v2")
        if optional_inputs["bucket"]:
            bucket_list = [optional_inputs["bucket"]]
        else:
            bucket_list = get_all_buckets()

        matched_files = []
        for bucket_name in bucket_list:
            try:
                s3.head_bucket(Bucket = bucket_name)
                track_api_call()
            except:
                logger.warning(f"Bucket {bucket_name} does not exist.")
                continue
                
            logger.info(f"Searching in bucket: {bucket_name}")

            page_iterator = paginator.paginate(Bucket = bucket_name)
            for page in page_iterator:
                track_api_call()
                if "Contents" in page:
                    for obj in page["Contents"]:
                        if obj["Key"].endswith("/") and obj["Size"] == 0: ## exclude folder as an object
                            continue
                        
                        can_be_added = True
                        for key in optional_inputs.keys():
                            if key in ["csv", "bucket"] or not optional_inputs[key]:
                                continue
                            elif key == "storageClass":
                                if optional_inputs[key] != obj["storageClass"]:
                                    can_be_added = False
                                    break
                            elif key == "match_substring":
                                pattern = optional_inputs[key]
                                if not re.search(pattern, obj["Key"]):
                                    can_be_added = False
                                    continue
                            else:
                                if not optional_inputs[key] in obj["Key"]:
                                    can_be_added = False
                                    break
                        if can_be_added:
                            matched_files.append([bucket_name, obj])

        if matched_files:
            print("\nMatching files found:")
            for file in matched_files:
                print(file, "\n")
            print(f"\n{len(matched_files)} files total found")    
        else:
            logger.info("\nNo matching files found.")

        return matched_files

    except Exception as e:
        logger.error(f"Error searching AWS: {e}")
        sys.exit()


def write_objects_to_restore(output_csv_path, matched_files):
    """Write found S3 objects to a CSV file."""
    with open(output_csv_path, mode="w", newline="") as the_new_csv:
        csv_writer = csv.writer(the_new_csv)
        csv_writer.writerow(["Bucket", "Object Key"])
        for bucket, obj_name in matched_files:
            csv_writer.writerow([bucket, obj_name])
    logger.info(f"\nFile '{output_csv_path}' has been created with {len(matched_files)} entries.")
    

def main():
    args = parse_args()
    if all(value is None for value in vars(args).values()):
        logger.warning(f"Please put in some parameters.")
        sys.exit()
    logger.info(f"Start searching AWS S3 ({strftime('%Y-%m-%d %H:%M:%S')})")
    matched_files = search_object(optional_inputs = vars(args))
    logger.info(f"Finish searching AWS S3 ({strftime('%Y-%m-%d %H:%M:%S')})")
    output_csv_path = args.csv
    if output_csv_path:
        write_objects_to_restore(output_csv_path, matched_files)
    logger.info(f"Total API calls made: {api_call_count}")

if __name__ == "__main__":
    main()