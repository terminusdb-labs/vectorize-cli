from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver
import boto3

session = boto3.Session()
credentials = session.get_credentials()

access_key = credentials.access_key
secret_key = credentials.secret_key

cls = get_driver(Provider.EC2)
driver = cls(access_key, secret_key)

print(driver.list_nodes())
