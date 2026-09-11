#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
stack_dir="$(cd "${script_dir}/.." && pwd)"

for required_command in aws terraform grep jq; do
  if ! command -v "${required_command}" >/dev/null 2>&1; then
    echo "Missing required command: ${required_command}" >&2
    exit 1
  fi
done

aws_region="${AWS_REGION:-$(terraform -chdir="${stack_dir}" output -raw aws_region)}"
cluster_name="$(terraform -chdir="${stack_dir}" output -raw ecs_cluster_name)"
name_prefix="$(terraform -chdir="${stack_dir}" output -raw name_prefix)"
recovery_timeout_seconds="${RECOVERY_TIMEOUT_SECONDS:-1800}"
poll_seconds="${RECOVERY_POLL_SECONDS:-10}"
http_enabled="$(terraform -chdir="${stack_dir}" output -raw http_enabled)"
native_target_groups="$(terraform -chdir="${stack_dir}" output -json native_target_group_arns)"
http_target_groups="$(terraform -chdir="${stack_dir}" output -json http_target_group_arns)"

for slot in node-0 node-1 node-2; do
  service_name="${name_prefix}-${slot}"
  family="${name_prefix}-${slot}"

  task_definition="$(
    aws ecs describe-task-definition \
      --region "${aws_region}" \
      --task-definition "${family}" \
      --query 'taskDefinition.taskDefinitionArn' \
      --output text
  )"

  native_target_group="$(jq -er --arg slot "${slot}" '.[$slot]' <<<"${native_target_groups}")"

  if [[ "${http_enabled}" == "true" ]]; then
    http_target_group="$(jq -er --arg slot "${slot}" '.[$slot]' <<<"${http_target_groups}")"
    load_balancers="$(
      jq -cn \
        --arg native "${native_target_group}" \
        --arg http "${http_target_group}" \
        '[
          {targetGroupArn: $native, containerName: "ferricstore", containerPort: 6388},
          {targetGroupArn: $http, containerName: "ferricstore", containerPort: 8080}
        ]'
    )"
  else
    load_balancers="$(
      jq -cn \
        --arg native "${native_target_group}" \
        '[{targetGroupArn: $native, containerName: "ferricstore", containerPort: 6388}]'
    )"
  fi

  echo "Deploying ${service_name} with ${task_definition}"

  aws ecs update-service \
    --region "${aws_region}" \
    --cluster "${cluster_name}" \
    --service "${service_name}" \
    --task-definition "${task_definition}" \
    --load-balancers "${load_balancers}" \
    --force-new-deployment \
    >/dev/null

  aws ecs wait services-stable \
    --region "${aws_region}" \
    --cluster "${cluster_name}" \
    --services "${service_name}"

  deadline=$((SECONDS + recovery_timeout_seconds))
  recovered=false

  while (( SECONDS < deadline )); do
    task_arn="$(
      aws ecs list-tasks \
        --region "${aws_region}" \
        --cluster "${cluster_name}" \
        --service-name "${service_name}" \
        --desired-status RUNNING \
        --query 'taskArns[0]' \
        --output text
    )"

    if [[ -n "${task_arn}" && "${task_arn}" != "None" ]]; then
      recovery_output="$(
        aws ecs execute-command \
          --region "${aws_region}" \
          --cluster "${cluster_name}" \
          --task "${task_arn}" \
          --container ferricstore \
          --interactive \
          --command "/app/bin/ferricstore-recovery-check" \
          2>&1 || true
      )"

      if grep -q "FERRICSTORE_RECOVERY_READY" <<<"${recovery_output}"; then
        recovered=true
        break
      fi
    fi

    sleep "${poll_seconds}"
  done

  if [[ "${recovered}" != true ]]; then
    echo "${service_name} did not fully catch up; refusing to stop the next node." >&2
    echo "Inspect CloudWatch logs and run Ferricstore.Cluster.Recovery.status() with ECS Exec." >&2
    exit 1
  fi

  echo "${service_name} is caught up. Continuing to the next slot."
done

echo "All three FerricStore node slots were deployed sequentially and recovered."
