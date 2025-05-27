# steps:

## SSH keys are better

**Setup SSH keys on GITHub!** Everything gets so much easier if you embrace SSH.
If you absolutely refuse to use SSH, then you will need a personal access token for the https URL.

## AWS

Ensure that you have selected the relevant profile and are logged into the AWS SSO.

WH: My login looks like this:

```bash
export AWS_PROFILE=prod-sub
aws sso login
```

## Deploying the Software

```bash
mkdir -p aws-object-search
cd aws-object-search

# One of these
git clone git@github.com:BCM-HGSC/aws-object-search.git repo
git clone https://github.com/BCM-HGSC/aws-object-search.git repo

# For production deployments, you must selet a software version:
VERSION=  # will default to "dev" if not set

# The $VERSION argument is optional and will default to "dev"
./repo/deploy $VERSION
```

The `deploy` script will ignore the contents of your home directory.
It will also ignore almost all environment variables.
This is accomplished through the `scripts/sanitize-command` script.

## Running the software

You could (and usually should) activate the environment you created in the previous section.
Instead it is possible to just run the software directly.

This is an example for a "dev" deployment that scans all buckets that start with "hgsc-b":

```bash
./aws-object-search-dev/bin/aos-scan --bucket-prefix hgsc-b
```

In production:

- cron jobs should invoke `aos-scan` by absolute path.
- The software should be deployed so that no activation is required for users to run `aos-search`.
