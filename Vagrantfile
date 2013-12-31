# -*- mode: ruby -*-
# vi: set ft=ruby :

# Vagrantfile API/syntax version. Don't touch unless you know what you're doing!
VAGRANTFILE_API_VERSION = "2"

Vagrant.configure("2") do |config|

  config.vm.define "wheezy" do |wheezy|
    wheezy.vm.box = "wheezy64"

    config.vm.provision "shell" do |s|
      s.path = "bootstrap-debian.sh"
      s.privileged = false
    end
  end

  config.vm.define "centos" do |centos|
    centos.vm.box = "CentOS64"

    config.vm.provision "shell" do |s|
      s.path = "bootstrap-centos.sh"
      s.privileged = false
    end
  end
end
