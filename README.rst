ApiMon
======

ApiMon is a project for testing OpenStack API availability by performing
Ansible playbooks that intend to provision/deprovision resources.

QuickStart
----------

The ``quickstart`` folder contains sample inventory that should be adapted to
the needs (at least fix IP addresses for the hosts and in the
``inventory/group_vars/all.yaml``). Installation is performed by Ansible using
Ansible Collection (as described in ``quickstart/README.md``
