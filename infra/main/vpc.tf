# VPC and networking for PayWatch.
#
# Layout: 1 VPC across 2 AZs (us-east-1a, us-east-1b), each AZ with one
# public subnet (internet-facing resources, e.g. a future EC2 app host)
# and one private subnet (RDS, ElastiCache - no direct internet access).
#
# No NAT gateway: the only things in the private subnets are RDS and
# ElastiCache, neither of which need outbound internet access. Skipping
# NAT avoids its hourly + per-GB cost, consistent with this project's
# cost-minimizing "EC2 + Docker Compose over EKS" decision.

resource "aws_vpc" "main" {
  cidr_block = "10.0.0.0/16"

  # RDS/ElastiCache endpoints are resolved by DNS hostname inside the VPC.
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = "${var.project_name}-vpc"
  }
}

# --- Public subnets ---------------------------------------------------
# Hold internet-facing resources. Instances launched here get a public IP
# automatically (map_public_ip_on_launch) and route to the internet via
# the internet gateway below.

resource "aws_subnet" "public_us_east_1a" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = "us-east-1a"
  map_public_ip_on_launch = true

  tags = {
    Name = "${var.project_name}-public-subnet-us-east-1a"
  }
}

resource "aws_subnet" "public_us_east_1b" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.2.0/24"
  availability_zone       = "us-east-1b"
  map_public_ip_on_launch = true

  tags = {
    Name = "${var.project_name}-public-subnet-us-east-1b"
  }
}

# --- Private subnets ------------------------------------------------------
# Hold RDS and ElastiCache. No route to the internet gateway, no NAT -
# these resources only need to be reachable from inside the VPC.

resource "aws_subnet" "private_us_east_1a" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.3.0/24"
  availability_zone = "us-east-1a"

  tags = {
    Name = "${var.project_name}-private-subnet-us-east-1a"
  }
}

resource "aws_subnet" "private_us_east_1b" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.4.0/24"
  availability_zone = "us-east-1b"

  tags = {
    Name = "${var.project_name}-private-subnet-us-east-1b"
  }
}

# --- Internet gateway -------------------------------------------------

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "${var.project_name}-igw"
  }
}

# --- Public route table: 0.0.0.0/0 -> internet gateway ----------------

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = {
    Name = "${var.project_name}-public-rt"
  }
}

resource "aws_route_table_association" "public_us_east_1a" {
  subnet_id      = aws_subnet.public_us_east_1a.id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "public_us_east_1b" {
  subnet_id      = aws_subnet.public_us_east_1b.id
  route_table_id = aws_route_table.public.id
}

# --- Private route table: local only (no NAT, no internet route) ------
#
# No explicit routes are needed here - every route table gets an implicit
# "local" route for the VPC's own CIDR (10.0.0.0/16) automatically, which
# is enough for RDS/ElastiCache to be reached from elsewhere in the VPC.
# A dedicated table (rather than leaving these subnets on the VPC's
# default route table) keeps private subnets isolated from ever picking
# up a future internet/NAT route added to the public table by mistake.

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "${var.project_name}-private-rt"
  }
}

resource "aws_route_table_association" "private_us_east_1a" {
  subnet_id      = aws_subnet.private_us_east_1a.id
  route_table_id = aws_route_table.private.id
}

resource "aws_route_table_association" "private_us_east_1b" {
  subnet_id      = aws_subnet.private_us_east_1b.id
  route_table_id = aws_route_table.private.id
}
