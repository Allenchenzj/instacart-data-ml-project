#!/bin/bash

aws glue list-sessions


ids=(
    "20c45fa5-cdbd-4cd5-9897-6d7b7144f4c6"
    "glue-studio-datapreview-35c93984-953b-4f65-9e3a-f5f92282a1f4"
)

for id in "${ids[@]}"; do
    echo "Stop session: $id"
    aws glue delete-session --id "$id"
done
