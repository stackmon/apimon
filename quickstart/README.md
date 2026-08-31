# Steps

. ``ansible-galaxy collection install git+https://github.com/opentelekomcloud/ansible-collection-apimon.git``
. adapt inventory
. ``ansible-playbook -i inventory ~/.ansible/collections/ansible_collections/opentelekomcloud/apimon/playbooks/install_scheduler.yaml``
. ``ansible-playbook -i inventory ~/.ansible/collections/ansible_collections/opentelekomcloud/apimon/playbooks/install_executor.yaml``
