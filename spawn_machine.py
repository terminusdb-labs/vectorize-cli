from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver
import boto3
from paramiko import AutoAddPolicy
from paramiko.client import SSHClient
from paramiko.ssh_exception import NoValidConnectionsError

session = boto3.Session()
credentials = session.get_credentials()

access_key = credentials.access_key
secret_key = credentials.secret_key

cls = get_driver(Provider.EC2)
driver = cls(access_key, secret_key)
IMAGE_ID = "ami-0b8e90c8e8faceeb8"
SIZE_ID = "g5.xlarge"
#SIZE_ID = "t3.micro"
SUBNET ="subnet-0141736fba5a518b1"
SG="sg-001bc2fe593af960e"

print('listing sizes..')
sizes = driver.list_sizes()
size = [s for s in sizes if s.id == SIZE_ID][0]
print(size)
print('get subnet..')
subnet = [ s for s in driver.ex_list_subnets() if s.id == SUBNET][0]
print(subnet)
print('get image..')
image = driver.get_image(IMAGE_ID)

print(image)

def get_ssh_conn():
    ssh_client = SSHClient()
    ssh_client.set_missing_host_key_policy(AutoAddPolicy())
    while True:
        try:
            ssh_client.connect(ip, username="ubuntu", timeout=1.0, )
            return ssh_client
        except TimeoutError:
            print('timeout.. retry')
            pass
        except NoValidConnectionsError:
            print('no valid connections.. retry')
            pass

node = driver.create_node(name="test-vectorize-autospawn",
                          image=image,
                          size=size,
                          ex_assign_public_ip=True,
                          ex_subnet=subnet,
                          #ex_terminate_on_shutdown=True,
                          ex_security_group_ids=[SG])
print('started!!')
print(node)

try:
    [(node, [ip])] = driver.wait_until_running([node], wait_period=5, timeout=600)
    print(f'running!! {node} {ip}')



    ssh_client = get_ssh_conn()
    _stdin, _stdout, _stderr = ssh_client.exec_command('./run strings.json strings.vec')
    #print(_stdout.read().decode())

    #_stdout.channel.shutdown_read()
    exit_status = _stdout.channel.recv_exit_status()
    print(exit_status)
    ssh_client.close()
finally:
    print(driver.destroy_node(node))

print('made it to the end')
