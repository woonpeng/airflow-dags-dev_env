#!/usr/bin/env bash
set -euo pipefail

function help {
  echo "Usage: add_ssh_key.sh  [-n] [-f authorized_keys_file] [-r rsa_pub_file]"
  echo "---------------------------------------"
  echo "-n:                         Creates a new authorized key file instead of appending to existing file"
  echo "-f authorized_keys_file:    The authorized_keys_file to insert the ssh key (default: ./neo4j_keys/authorized_keys)"
  echo "-r rsa_pub_file:            The RSA public key file to allow access to [default: ~/.ssh/id_rsa.pub]"
  echo "--------------------------------------"
  echo "You can generate a ssh key pair by 'ssh-keygen -t rsa -b 4096 -C "your_email@example.com"'"
}

# Set defaults
user=root
authorized_keys_file=./neo4j_keys/authorized_keys
rsa_pub_file=~/.ssh/id_rsa.pub
flag_newfile=0

while getopts ":hn:f:r:" opt; do
  case ${opt} in
    n )
      flag_newfile=1
      ;;
    f )
      authorized_keys_file=$OPTARG
      ;;
    r )
      rsa_pub_file=$OPTARG
      ;;
    \? )
      help
      exit -1
      ;;
    : )
      help
      exit -1
      ;;
    h )
      help
      exit -1
      ;;
  esac
done

# Check to make sure that the authorized_keys file does not already exist 
# and if overwrite is not specified
if [[ -f "$authorized_keys_file" ]] && [ $flag_newfile -eq 0 ]; 
then
  echo "$authorized_keys_file" already exists, make sure that it was created with '-n' previously to set the correct permissions 
fi

# Give a warning if authorized_keys file does not exist and '-n' option not specified
if [[ ! -f "$authorized_keys_file" ]]; 
then 
  echo "$authorized_keys_file" does not already exists, creating it...
  flag_newfile=1
fi

restrictions='no-port-forwarding,no-X11-forwarding,no-agent-forwarding,no-pty,command="remote-access-ctl.sh"'
rsa=$(cat $rsa_pub_file)
authorized_keys="$restrictions $rsa"
if [ $flag_newfile -eq 1 ];
then
  echo "$authorized_keys">$authorized_keys_file
else
  echo "$authorized_keys">>$authorized_keys_file
fi

echo Added "$rsa_pub_file" into the file "$authorized_keys_file"
echo Mount or copy "$authorized_keys_file" into the '.ssh' folder in the neo4j image 
echo e.g. /root/.ssh/authorized_keys for 'root' user 
echo e.g. /home/neo4j/.ssh/authorized_keys for 'neo4j' user
