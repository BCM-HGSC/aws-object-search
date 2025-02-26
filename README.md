This repository contains a script for searching files stored in AWS (Amazon Web Services) S3 buckets. It works with all AWS storage classes, including Glacier, Standard, and more.

Before using the script, make sure you have:
1. Python installed on your computer (Check by run "python3 --version
")
2. The script uses boto3 library to interact with AWS. Install botos by "pip install boto3"

How to use the script:

python searchGlacier.py -b <BUCKET_NAME> -p <PLATFORM> -t <TECHNOLOGY>

Required Parameters
-b or --bucket: (Required) Name of the AWS S3 bucket
-p or --platform: (Required) Platform name (e.g., Illumina)
-t or --technology: (Required) Technology type (e.g., wholegenome)
Optional Parameters
-c or --pathContains: (Optional) Keyword to filter file paths (Flowcell-Lane-Barcode)
-s or --storageClass: (Optional) Expected storage class of files

Example command:
1. python searchGlacier.py -b my-bucket -p Illumina -t wholegenome
2. python searchGlacier.py -b my-bucket -p Illumina -t wholegenome -c HABCDE-1-IDU12345

Expected output 1:

Matching files found:
{'Key': 'Illumina/wholegenome/sample1.fastq.gz', 'Size': 12345678}
{'Key': 'Illumina/wholegenome/sample2.fastq.gz', 'Size': 98765432}

2 files total found in my-bucket

Expecte output 2:

No matching files found.