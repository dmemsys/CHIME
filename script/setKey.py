import os

user = 'xuchuan'

server_list = [
    'clnode279.clemson.cloudlab.us',
    'clnode281.clemson.cloudlab.us',
    'clnode264.clemson.cloudlab.us',
    'clnode259.clemson.cloudlab.us',
    'clnode267.clemson.cloudlab.us',
    'clnode271.clemson.cloudlab.us',
    'clnode278.clemson.cloudlab.us',
    'clnode270.clemson.cloudlab.us',
    'clnode282.clemson.cloudlab.us',
    'clnode274.clemson.cloudlab.us',
]

# generate keys
for s in server_list:
    cmd = f'ssh {user}@{s} -o StrictHostKeyChecking=no "ssh-keygen -t rsa -b 2048 -N \'\' -f ~/.ssh/id_rsa"'
    os.system(cmd)

pub_key_list = []
for s in server_list:
    cmd = f'scp -o StrictHostKeyChecking=no {user}@{s}:.ssh/id_rsa.pub ./'
    os.system(cmd)
    with open('id_rsa.pub', 'r') as f:
        pub_key_list.append(f.readline())

cmd = f'scp -o StrictHostKeyChecking=no {user}@{server_list[0]}:.ssh/authorized_keys ./'
os.system(cmd)
with open('authorized_keys', 'a') as f:
    for key in pub_key_list:
        f.write(key)

for s in server_list:
    cmd = f'scp -o StrictHostKeyChecking=no ./authorized_keys {user}@{s}:.ssh/authorized_keys'
    os.system(cmd)
