#!/bin/sh
# wait for few seconds to prepare scheduler for the run
sleep 5

#get optimus version
echo "-- optimus client version"
/opt/optimus version

# get resources
echo "-- initializing optimus assets"
OPTIMUS_ADMIN_ENABLED=1 /opt/optimus admin build instance "$JOB_NAME" --project-name \
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

echo "-- exporting env with secret"
set -o allexport
source "$JOB_DIR/in/.secret"
set +o allexport

echo "-- running unit"
exec $(eval echo "$@")
