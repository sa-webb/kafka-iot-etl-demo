from boto3 import client
from botocore.exceptions import ClientError
from configparser import ConfigParser


def get_obj_uris(s3, bucket: str, prefix: str) -> list[str]:
    """
    Get object URI's from S3.

    Args:
        s3: S3 client object.
        bucket (str): S3 bucket name.
        prefix (str): S3 prefix.

    Returns:
        list[str]: Unpaginated list of object URI's.
    """

    objs = [o for o in s3.list_objects(
        Bucket=bucket, Prefix=prefix)['Contents']]
    return [o['Key'] for o in objs]


def format_uris(obj_uris: list[str]) -> list[str]:
    """
    Format URI's for Glue job and pushdown predicate.

    Args:
        obj_uris (list[str]): List of object URI's.

    Returns:
        list[str]: 
    """
    raw_uris = [o.split('/')[2:5] for o in obj_uris]
    uris = []
    for uri in raw_uris:
        year = uri[0]
        month = uri[1]
        day = uri[2]
        uris.append(f"/{year}/{month}/{day}")
    return uris


def format_predicate(uri: str) -> str:
    """Formats a URI for a pushdown predicate.

    Args:
        uri (str): the URI to format.

    Returns:
        str: predicate
    """
    year = None
    month = None
    day = None
    return "year = \'{}\' and month = \'{}\' and day=\'{}\'".format(year, month, day)


def run_glue_job(glue: dict, job_name: str, arguments: dict = {}) -> str:
    """
    Starts a Glue job by passing it the job name and arguments.

    Args:
        client (dict): Glue client object.
        job_name (str): Job name.
        arguments (dict, optional): Arguments for the job. Defaults to {}.

    Raises:
        Exception: ClientError from boto3.
        Exception: Unexpected error.

    Returns:
        str: Job status.
    """
    try:
        job_run_id = glue.start_job_run(
            JobName=job_name, Arguments=arguments)

        status_detail = glue.get_job_run(
            JobName=job_name, RunId=job_run_id["JobRunId"])

        return status_detail.get("JobRun").get("JobRunState")

    except ClientError as e:
        raise Exception(
            "boto3 client error in run_glue_job_get_status: " + e.__str__())
    except Exception as e:
        raise Exception(
            "Unexpected error in run_glue_job_get_status: " + e.__str__())


# Driver code.
if __name__ == "__main__":
    # Read config via .ini file
    config = ConfigParser()
    config.read('config.ini')
    BUCKET = config.get('aws', 'bucket')
    TOPIC = config.get('aws', 'topic')
    REGION = config.get('aws', 'region')
    JOB_PREFIX = config.get('aws', 'job_prefix')
    DATABASE = config.get('aws', 'database')
    TABLE = config.get('aws', 'table')

    # Create S3 client
    s3 = client('s3', region_name=REGION)
    
    # Get object URI's
    objs = get_obj_uris()

    # Format URI's for Glue job
    jobs = format_uris(objs)

    # Format your pushdown predicates 
    pushdown_predicate = format_predicate(jobs[0])
    
    year = None
    month = None
    day = None
    
    job_name = JOB_PREFIX + year + month + day

    # Prepare Glue Job
    
    args = {
        "--job": job_name,
        "--db": DATABASE,
        "--table": TABLE,
        "--topic": TOPIC,
        "--pushdown_predicate": pushdown_predicate,
        "--year": year,
        "--month": month,
        "--day": day,

    }

    glue = client('glue')
    
    run = run_glue_job(glue, job_name, args)
    print(f'Status: {run} for {args}')
