# Reconnect env for dbt Redshift DEV
export AWS_PROFILE=dbt-dev-redshift

SECRET_JSON=$(aws secretsmanager get-secret-value \
  --secret-id dev/dbt/redshift \
  --query SecretString --output text)

if command -v jq >/dev/null 2>&1; then
  export REDSHIFT_USER_DEV=$(jq -r .username <<<"$SECRET_JSON")
  export REDSHIFT_PASSWORD_DEV=$(jq -r .password <<<"$SECRET_JSON")
  export REDSHIFT_HOST_DEV=$(jq -r .host <<<"$SECRET_JSON")
else
  export REDSHIFT_USER_DEV="$(python3 -c 'import json,sys; s=json.load(sys.stdin); print(s.get("username",""))' <<< "$SECRET_JSON")"
  export REDSHIFT_PASSWORD_DEV="$(python3 -c 'import json,sys; s=json.load(sys.stdin); print(s.get("password",""))' <<< "$SECRET_JSON")"
  export REDSHIFT_HOST_DEV="$(python3 -c 'import json,sys; s=json.load(sys.stdin); print(s.get("host",""))' <<< "$SECRET_JSON")"
fi

export REDSHIFT_DB_DEV="nyc_taxi_db"

echo "Redshift DEV env set:"
echo "  HOST=$REDSHIFT_HOST_DEV"
echo "  DB=$REDSHIFT_DB_DEV"
echo "  USER=$REDSHIFT_USER_DEV"
