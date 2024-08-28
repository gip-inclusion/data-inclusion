#!/bin/sh
set -u

"$@" &
command_pid=$! 
wait $command_pid

exit_code=$?  
if [ $exit_code -ne 0 ] && [ "$ENV" = "prod" ]; then
    echo "Error with exit code \`$exit_code\` in command \`$*\`, sending message to Mattermost"
    curl -i -X POST -H "Content-Type: application/json" -d "{\"text\": \"Error with exit code \`$exit_code\` in command \`$*\`:  \nLogs: \`scalingo --region osc-secnum-fr1 --app data-inclusion-api-prod logs --lines 1000 -F $CONTAINER -f\`\"}" $MATTERMOST_HOOK
fi

exit $exit_code
