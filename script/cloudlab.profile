"""10 r650 instances for running CHIME"""

#
# NOTE: This code was machine converted. An actual human would not
#       write code like this!
#

# Import the Portal object.
import geni.portal as portal
# Import the ProtoGENI library.
import geni.rspec.pg as pg
# Import the Emulab specific extensions.
import geni.rspec.emulab as emulab

# Create a portal object,
pc = portal.Context()

# Create a Request object to start building the RSpec.
request = pc.makeRequestRSpec()

# Node node-0
node_0 = request.RawPC('node-0')
node_0.routable_control_ip = True
node_0.hardware_type = 'r650'
node_0.disk_image = 'urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU18-64-STD'
iface0 = node_0.addInterface('interface-1')

# Node node-1
node_1 = request.RawPC('node-1')
node_1.routable_control_ip = True
node_1.hardware_type = 'r650'
node_1.disk_image = 'urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU18-64-STD'
iface1 = node_1.addInterface('interface-0')

# Node node-2
node_2 = request.RawPC('node-2')
node_2.routable_control_ip = True
node_2.hardware_type = 'r650'
node_2.disk_image = 'urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU18-64-STD'
iface2 = node_2.addInterface('interface-2')

# Node node-3
node_3 = request.RawPC('node-3')
node_3.routable_control_ip = True
node_3.hardware_type = 'r650'
node_3.disk_image = 'urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU18-64-STD'
iface3 = node_3.addInterface('interface-3')

# Node node-4
node_4 = request.RawPC('node-4')
node_4.routable_control_ip = True
node_4.hardware_type = 'r650'
node_4.disk_image = 'urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU18-64-STD'
iface4 = node_4.addInterface('interface-4')

# Node node-5
node_5 = request.RawPC('node-5')
node_5.routable_control_ip = True
node_5.hardware_type = 'r650'
node_5.disk_image = 'urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU18-64-STD'
iface5 = node_5.addInterface('interface-5')

# Node node-6
node_6 = request.RawPC('node-6')
node_6.routable_control_ip = True
node_6.hardware_type = 'r650'
node_6.disk_image = 'urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU18-64-STD'
iface6 = node_6.addInterface('interface-6')

# Node node-7
node_7 = request.RawPC('node-7')
node_7.routable_control_ip = True
node_7.hardware_type = 'r650'
node_7.disk_image = 'urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU18-64-STD'
iface7 = node_7.addInterface('interface-7')

# Node node-8
node_8 = request.RawPC('node-8')
node_8.routable_control_ip = True
node_8.hardware_type = 'r650'
node_8.disk_image = 'urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU18-64-STD'
iface8 = node_8.addInterface('interface-8')

# Node node-9
node_9 = request.RawPC('node-9')
node_9.routable_control_ip = True
node_9.hardware_type = 'r650'
node_9.disk_image = 'urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU18-64-STD'
iface9 = node_9.addInterface('interface-9')


# Link link-0
link_0 = request.Link('link-0')
link_0.Site('undefined')
link_0.addInterface(iface1)
link_0.addInterface(iface0)
link_0.addInterface(iface2)
link_0.addInterface(iface3)
link_0.addInterface(iface4)
link_0.addInterface(iface5)
link_0.addInterface(iface6)
link_0.addInterface(iface7)
link_0.addInterface(iface8)
link_0.addInterface(iface9)


# Print the generated rspec
pc.printRequestRSpec(request)