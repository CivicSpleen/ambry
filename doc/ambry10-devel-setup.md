

    apt-get update && apt-get install -y curl  && apt-get upgrade gcc
    bash -c "$(curl -fsSL https://raw.githubusercontent.com/CivicKnowledge/ambry/ambry1.0/support/prepare-ubuntu.sh)"
    cd /opt
    git clone https://github.com/CivicKnowledge/ambry.git
    cd ambry
    git checkout ambry1.0
    python setup.py develop
    
    
For Ubunty trusty 14.0, may also need to relax the versino of numpy to the default, 1.8.2. 1.9 seemed to have trouble compiling. 

    