

#!/bin/bash
# deploy-services.sh

set -e

SERVICES=("nginx" "personal-website" "filebeat" "kafka")
if [[ $(hostname) == "kafka1" ]]; then
  SERVICES+=("log-consumer" "redis")
fi

for svc in "${SERVICES[@]}"; do
  echo "Enabling and starting $svc..."
  systemctl enable --now "$svc"
done

echo "✅ All services started!"
systemctl list-units --type=service --state=active | grep -E 'nginx|personal|filebeat|kafka|log|redis'
