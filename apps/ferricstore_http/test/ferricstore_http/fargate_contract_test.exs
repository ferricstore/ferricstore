defmodule FerricstoreHttp.FargateContractTest do
  use ExUnit.Case, async: true

  @repo_root Path.expand("../../../..", __DIR__)

  test "single-task Fargate exposes an opt-in HTTP or TLS endpoint" do
    stack = read_stack("fargate")

    assert_http_inputs_are_opt_in(stack.variables)
    assert_http_task_wiring(stack.main)
    assert stack.main =~ ~s(resource "aws_lb_target_group" "http")
    assert stack.main =~ ~s(path                = "/health/ready")
    assert stack.main =~ ~s(resource "aws_lb_listener" "http")
    assert stack.main =~ ~s(resource "aws_lb_listener" "https")
    assert stack.main =~ ~s(dynamic "load_balancer")
    assert stack.outputs =~ ~s(output "http_endpoint")
    assert stack.outputs =~ ~s(output "http_readiness_endpoint")
    assert stack.variables =~ "quay.io/ferricstore/ferricstore:0.11.13"
  end

  test "cluster Fargate follows stable slots and guards HTTP target-group changes" do
    stack = read_stack("fargate-cluster")
    rollout = File.read!(stack.rollout)

    assert_http_inputs_are_opt_in(stack.variables)
    assert_http_task_wiring(stack.main)
    assert stack.main =~ ~s(resource "aws_lb_target_group" "http")
    assert stack.main =~ ~s(for_each = var.http_enabled ? local.node_slots : {})
    assert stack.main =~ ~s(path                = "/health/live")
    assert stack.main =~ ~s(resource "aws_lb_listener" "http")
    assert stack.main =~ ~s(resource "aws_lb_listener" "https")
    assert stack.main =~ ~s(aws_lb_target_group.http[each.key].arn)
    assert stack.outputs =~ ~s(output "http_target_group_arns")
    assert rollout =~ ~s(--load-balancers "${load_balancers}")
    assert rollout =~ ~s(output -raw http_enabled)
    assert rollout =~ ~s(output -json http_target_group_arns)
  end

  test "Fargate documentation states the authentication and TLS boundaries" do
    single = File.read!(Path.join(@repo_root, "deploy/aws/fargate/README.md"))
    cluster = File.read!(Path.join(@repo_root, "deploy/aws/fargate-cluster/README.md"))

    for document <- [single, cluster] do
      assert document =~ "HTTP Basic"
      assert document =~ "Create An HTTP User"
      assert document =~ "ACL\", \"SETUSER"
      assert document =~ "security guide"
      assert document =~ "Do not put the plaintext password in Terraform variables"
      assert document =~ "http_tls_certificate_arn"
      assert document =~ "plaintext"
      assert document =~ "private"
    end

    assert cluster =~ "Enabling Or Disabling HTTP On A Running Cluster"
    assert cluster =~ "deploy-sequential.sh"
  end

  defp read_stack(name) do
    root = Path.join([@repo_root, "deploy", "aws", name])

    %{
      main: File.read!(Path.join(root, "main.tf")),
      outputs: File.read!(Path.join(root, "outputs.tf")),
      variables: File.read!(Path.join(root, "variables.tf")),
      rollout: Path.join(root, "scripts/deploy-sequential.sh")
    }
  end

  defp assert_http_inputs_are_opt_in(variables) do
    assert variables =~ ~s(variable "http_enabled")
    assert variables =~ ~r/variable "http_enabled" \{.*?default\s+= false/s
    assert variables =~ ~s(variable "http_listener_port")
    assert variables =~ ~s(variable "http_tls_certificate_arn")
  end

  defp assert_http_task_wiring(main) do
    assert main =~ ~s(name          = "http-api")
    assert main =~ ~s(name = "FERRICSTORE_HTTP_ENABLED")
    assert main =~ ~s(name = "FERRICSTORE_HTTP_BIND")
    assert main =~ ~s(name = "FERRICSTORE_HTTP_PORT")
    assert main =~ ~s(preserve_client_ip = true)
  end
end
