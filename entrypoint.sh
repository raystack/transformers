#!/bin/sh
# wait for few seconds to prepare scheduler for the run
sleep 5

#get optimus version
echo "-- optimus client and server version"
/opt/optimus version --with-server

# get resources
echo "-- initializing optimus assets"
OPTIMUS_ADMIN_ENABLED=1 /opt/optimus admin build instance "$JOB_NAME" --project \
"$PROJECT" --output-dir "$JOB_DIR" \
--type "$INSTANCE_TYPE" --name "$INSTANCE_NAME" \
--scheduled-at "$SCHEDULED_AT" --host "$OPTIMUS_HOST"

# TODO: this doesnt support using back quote sign in env vars, fix it
echo "-- exporting env"
set -o allexport
source "$JOB_DIR/in/.env"
set +o allexport

echo "-- current envs"
printenv

echo "-- running unit"
exec $(eval echo "$@")
