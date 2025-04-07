This repository contains a script for searching files stored in AWS (Amazon Web Services) S3 buckets. It works with all AWS storage classes, including Glacier, Standard, and more.


## How to use the script:

### python searchGlacier.py -b <BUCKET_NAME> -m <STRING>

Optional Parameters <br>
    -f, --flowcell-lane-barcode     (Optional) Search flowcell-lane-barcode, e.g. HABCDEFG-3-IDU000000 <br>
    -b, --bucket                    (Optional) Name of the AWS S3 bucket <br>
    -p, --platform                  (Optional) Name of platform (e.g. Illumina) <br>
    -t, --technology                (Optional) Name of technology type (e.g. wholegenome) <br>
    -s, --storageClass              (Optional) Expected storage class of the files <br>
    -m, --match-substring           (Optional) Part of the path name or file name <br>
    -c, --csv                       (Optional) Full path of the output csv

Example command:
1. python searchGlacier.py -f HABCDED-3-IDUD001234 -b hgsc-dev
2. python searchGlacier.py -b hgsc-alzheimers -m BCM_ABCD_EDGFI_SIC1234_1.cram.crai
3. python searchGlacier.py -b hgsccl-czi -m "SIC1234.*fastq_list"

Expected output 1:

Matching files found: <br>
{'Key': 'Illumina/wholegenome/sample1.fastq.gz', 'Size': 12345678} <br>
{'Key': 'Illumina/wholegenome/sample2.fastq.gz', 'Size': 98765432} <br>

2 files total found in my-bucket

Expecte output 2:

No matching files found.

## If the script does not show the expected outcome:
1. Check if the AWS credential is correctly activated
2. Check if input the correct parameters