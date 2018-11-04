while inotifywait -r -e modify,create,delete /home/mafacz/source/VAP; do
    rsync -avz --delete /home/mafacz/source/VAP mafacz@leomed:/cluster/home/mafacz/source_remote_copy
done
