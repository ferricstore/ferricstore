terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }
}

provider "azurerm" {
  features {}
}

# ===========================================================================
# Resource Group
# ===========================================================================

resource "azurerm_resource_group" "bench" {
  name     = var.resource_group_name
  location = var.location
}

# ===========================================================================
# Network
# ===========================================================================

resource "azurerm_virtual_network" "bench" {
  name                = "ferricstore-vnet"
  address_space       = ["10.0.0.0/16"]
  location            = azurerm_resource_group.bench.location
  resource_group_name = azurerm_resource_group.bench.name
}

resource "azurerm_subnet" "bench" {
  name                 = "ferricstore-subnet"
  resource_group_name  = azurerm_resource_group.bench.name
  virtual_network_name = azurerm_virtual_network.bench.name
  address_prefixes     = ["10.0.1.0/24"]
}

resource "azurerm_network_security_group" "bench" {
  name                = "ferricstore-nsg"
  location            = azurerm_resource_group.bench.location
  resource_group_name = azurerm_resource_group.bench.name

  security_rule {
    name                       = "SSH"
    priority                   = 1001
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "22"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }

  security_rule {
    name                       = "FerricStore"
    priority                   = 1002
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = tostring(var.ferricstore_port)
    source_address_prefix      = "10.0.0.0/16"
    destination_address_prefix = "*"
  }

  # Erlang distribution port range (for cluster)
  security_rule {
    name                       = "ErlangDist"
    priority                   = 1003
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "4369-4380"
    source_address_prefix      = "10.0.0.0/16"
    destination_address_prefix = "*"
  }

  # epmd + ra inter-node (dynamic ports)
  security_rule {
    name                       = "ErlangPorts"
    priority                   = 1004
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "9100-9200"
    source_address_prefix      = "10.0.0.0/16"
    destination_address_prefix = "*"
  }
}

# ===========================================================================
# VMs
# ===========================================================================

resource "azurerm_public_ip" "bench" {
  count               = var.node_count
  name                = "ferricstore-ip-${count.index}"
  location            = azurerm_resource_group.bench.location
  resource_group_name = azurerm_resource_group.bench.name
  allocation_method   = "Static"
  sku                 = "Standard"
}

resource "azurerm_network_interface" "bench" {
  count               = var.node_count
  name                = "ferricstore-nic-${count.index}"
  location            = azurerm_resource_group.bench.location
  resource_group_name = azurerm_resource_group.bench.name

  ip_configuration {
    name                          = "internal"
    subnet_id                     = azurerm_subnet.bench.id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.bench[count.index].id
  }
}

resource "azurerm_network_interface_security_group_association" "bench" {
  count                     = var.node_count
  network_interface_id      = azurerm_network_interface.bench[count.index].id
  network_security_group_id = azurerm_network_security_group.bench.id
}

resource "azurerm_linux_virtual_machine" "bench" {
  count               = var.node_count
  name                = "ferricstore-${count.index}"
  location            = azurerm_resource_group.bench.location
  resource_group_name = azurerm_resource_group.bench.name
  size                = var.vm_size
  admin_username      = var.admin_username

  network_interface_ids = [
    azurerm_network_interface.bench[count.index].id
  ]

  admin_ssh_key {
    username   = var.admin_username
    public_key = file(var.ssh_public_key_path)
  }

  os_disk {
    caching              = "ReadOnly"
    storage_account_type = "Premium_LRS"
    disk_size_gb         = 30
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "ubuntu-24_04-lts"
    sku       = "server"
    version   = "latest"
  }

  custom_data = base64encode(templatefile("${path.module}/cloud-init.yaml", {
    otp_version      = var.otp_version
    elixir_version   = var.elixir_version
    shard_count      = var.shard_count
    ferricstore_port = var.ferricstore_port
    node_index       = count.index
    node_count       = var.node_count
    admin_username   = var.admin_username
    data_filesystem  = var.data_filesystem
    git_branch       = var.ferricstore_git_branch
  }))

  tags = {
    environment = "benchmark"
    project     = "ferricstore"
  }
}

# ===========================================================================
# Benchmark client VM (separate to avoid competing for resources)
# ===========================================================================

resource "azurerm_public_ip" "client" {
  name                = "ferricstore-client-ip"
  location            = azurerm_resource_group.bench.location
  resource_group_name = azurerm_resource_group.bench.name
  allocation_method   = "Static"
  sku                 = "Standard"
}

resource "azurerm_network_interface" "client" {
  name                = "ferricstore-client-nic"
  location            = azurerm_resource_group.bench.location
  resource_group_name = azurerm_resource_group.bench.name

  ip_configuration {
    name                          = "internal"
    subnet_id                     = azurerm_subnet.bench.id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.client.id
  }
}

resource "azurerm_network_interface_security_group_association" "client" {
  network_interface_id      = azurerm_network_interface.client.id
  network_security_group_id = azurerm_network_security_group.bench.id
}

resource "azurerm_linux_virtual_machine" "client" {
  name                = "ferricstore-client"
  location            = azurerm_resource_group.bench.location
  resource_group_name = azurerm_resource_group.bench.name
  size                = var.client_vm_size
  admin_username      = var.admin_username

  network_interface_ids = [
    azurerm_network_interface.client.id
  ]

  admin_ssh_key {
    username   = var.admin_username
    public_key = file(var.ssh_public_key_path)
  }

  os_disk {
    caching              = "ReadOnly"
    storage_account_type = "Premium_LRS"
    disk_size_gb         = 30
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "ubuntu-24_04-lts"
    sku       = "server"
    version   = "latest"
  }

  custom_data = base64encode(templatefile("${path.module}/cloud-init-client.yaml", {
    otp_version    = var.otp_version
    elixir_version = var.elixir_version
    admin_username = var.admin_username
    git_branch     = var.ferricstore_git_branch
  }))

  tags = {
    environment = "benchmark"
    project     = "ferricstore"
    role        = "client"
  }
}
