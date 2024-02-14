githooks enable a user to have things happen automatically based on the git action taken.  DataWave has the following hooks:

pre-push
Before pushing the code up to github, this script will 
RUN:
  ln -fns githooks/pre-push .git/hooks/pre-push 
