# -*- mode: ruby -*-
# vi: set ft=ruby :

# Vagrantfile API/syntax version. Don't touch unless you know what you're doing!
VAGRANTFILE_API_VERSION = "2"

Vagrant.configure("2") do |config|

  config.vm.define "wheezy" do |wheezy|
    wheezy.vm.box = "wheezy64"

    config.vm.provision :file do |file|
      file.source      = 'conf/gpg-agent.conf'
      file.destination = '/home/vagrant/.gnupg/gpg-agent.conf'
    end

    config.vm.provision :file do |file|
      file.source      = 'conf/gpg.conf'
      file.destination = '/home/vagrant/.gnupg/gpg.conf'
    end

    config.vm.provision :file do |file|
      file.source      = 'conf/devscripts'
      file.destination = '/home/vagrant/.devscripts'
    end

    config.vm.provision "shell" do |s|
      s.path = "bootstrap-debian.sh"
      s.privileged = false
    end

    config.vm.network :forwarded_port, guest: 4505, host: 4505
  end

  config.vm.define "centos" do |centos|
    centos.vm.box = "CentOS64"

    config.vm.provision "shell" do |s|
      s.path = "bootstrap-centos.sh"
      s.privileged = false
    end
  end
end
