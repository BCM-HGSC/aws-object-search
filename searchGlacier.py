#!/usr/bin/env python3
import boto3
import argparse
import sys
import logging
from time import strftime

"""
Search AWS Glacier bucket files based on user's inputs

Example command:
python searchGlacier.py -b hgsc-gregor -p Illumina -t wholegenome
"""

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO, format="%(message)s")

def parse_args():
    """ CLI interface to intake inputs
    RETURNS: args
    """
    parser = argparse.ArgumentParser(
        usage = "Example command: python searchGlacier.py -b hgsc-gregor -p Illumina -t wholegenome"
        )
    parser.add_argument(
        "-b",
        "--bucket", 
        required = True, 
        help = "Name of the AWS S3 bucket"
        )
    parser.add_argument(
        "-p",
        "--platform", 
        required = True, 
        help = "Name of platform (e.g. Illumina)"
        )
    parser.add_argument(
        "-t",
        "--technology", 
        required = True, 
        help = "Name of technology type (e.g. wholegenome)"## ,
        ## choices=["wholegenome"] #MING define all the valid options
        )
    parser.add_argument(
        "-c",
        "--pathContains",
        help = "(Optional) Search keyword, e.g. Flowcell-Lane-Barcode"
        )
    parser.add_argument(
        "-s",
        "--storageClass", 
        help = "Expected storage class of the files"
        )

    try:
        args = parser.parse_args()
    except:
        logger.error("The input parameters are not valid")
        sys.exit()
    return args

def search_object(bucket_name, platform, technology, path_contains):
    """Search AWS bucket based on the inputs from parse_args()"""
    try:
        s3 = boto3.client("s3")
        paginator = s3.get_paginator("list_objects_v2")
        #prefix = f"v1/{platform}/{technology}/fastqs"
        prefix = f'{platform}/{technology}' # for testing
        page_iterator = paginator.paginate(Bucket = bucket_name, Prefix = prefix)
        matched_files = []
        for page in page_iterator:
            if "Contents" in page:
                for obj in page["Contents"]:
                    if obj["Key"].endswith('/') and obj["Size"] == 0: ## exclude folder as an object
                        continue
                    if path_contains:
                        if not path_contains in obj["Key"]:
                            continue
                    matched_files.append(obj)
        if matched_files:
            print("\nMatching files found:")
            for file in matched_files:
                print(file)
            print(f"\n{len(matched_files)} files total found in {bucket_name}")
        else:
            print("\nNo matching files found.")
            
    except Exception as e:
        logger.error(f"Error searching AWS: {e}") #i.e. ERROR:root:Error searching aws: Unable to locate credentials
        sys.exit()


def main():
    args = parse_args()
    logger.debug("Arguments:", args)
    logger.info(f"Start searching aws buckets ({strftime('%Y-%m-%d %H:%M:%S')})")
    search_object(args.bucket, args.platform, args.technology, args.pathContains)
    logger.info(f"\nFinish searching aws buckets ({strftime('%Y-%m-%d %H:%M:%S')})")

if __name__ == "__main__":
    main()