data "aws_ami" "ecs_optimized" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-ecs-hvm-*-x86_64"]
  }
}

resource "aws_ebs_volume" "airflow_data" {
  availability_zone = var.az
  size              = var.data_volume_gb
  type              = "gp3"
  encrypted         = true

  tags = { Name = "${local.name}-postgres-data" }

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_launch_template" "airflow" {
  name_prefix   = "${local.name}-lt-"
  image_id      = data.aws_ami.ecs_optimized.id
  instance_type = var.instance_type
  key_name      = var.key_name != "" ? var.key_name : null

  iam_instance_profile {
    name = aws_iam_instance_profile.ec2_airflow_host.name
  }

  vpc_security_group_ids = [aws_security_group.airflow_host.id]

  block_device_mappings {
    device_name = "/dev/xvda"
    ebs {
      volume_size           = var.root_volume_gb
      volume_type           = "gp3"
      encrypted             = true
      delete_on_termination = true
    }
  }

  metadata_options {
    http_tokens                 = "required"
    http_endpoint               = "enabled"
    http_put_response_hop_limit = 2 # containers need IMDS too
  }

  user_data = base64encode(templatefile("${path.module}/../user_data/bootstrap.sh", {
    cluster_name   = aws_ecs_cluster.airflow.name
    data_volume_id = aws_ebs_volume.airflow_data.id
    aws_region     = var.aws_region
    repo_url       = var.repo_url
  }))

  tag_specifications {
    resource_type = "instance"
    tags          = { Name = "${local.name}-host" }
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_autoscaling_group" "airflow" {
  name                = "${local.name}-asg"
  min_size            = 1
  max_size            = 1
  desired_capacity    = 1
  vpc_zone_identifier = [aws_subnet.public.id]

  launch_template {
    id      = aws_launch_template.airflow.id
    version = "$Latest"
  }

  health_check_type         = "EC2"
  health_check_grace_period = 180

  # Required by the ECS capacity provider to be able to drain instances.
  protect_from_scale_in = true

  tag {
    key                 = "Name"
    value               = "${local.name}-host"
    propagate_at_launch = true
  }

  tag {
    key                 = "AmazonECSManaged"
    value               = ""
    propagate_at_launch = true
  }
}
