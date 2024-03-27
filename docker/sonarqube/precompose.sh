#!/bin/bash
sudo sysctl -w vm.max_map_count=524288
sudo sysctl -w fs.file-max=131072

