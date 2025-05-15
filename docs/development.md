steps:

```bash
mkdir aws-object-search
cd aws-object-search
git clone git@github.com:BCM-HGSC/aws-object-search.git repo
mkdir s3_objects
cd repo
git switch wh-dev

bash repo/scripts/fetch-micromamba.sh

./bin/micromamba create -p aws-object-search-dev/env python tantivy tantivy-py boto3

./bin/micromamba env export -p aws-object-search-dev/env
cd repo
../aws-object-search-dev/env/bin/pip install -e .

export AWS_PROFILE=prod-sub
aws sso login
./aws-object-search-dev/env/bin/aos-scan --bucket-prefix hgsc-a

fgrep fastq.gz
```
