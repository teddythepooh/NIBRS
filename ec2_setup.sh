sudo apt install git

if [[ ! -d ~/miniconda3 ]]; then
    echo 'Downloading and configuring miniconda...'
    
    mkdir -p ~/miniconda3
    wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda3/miniconda.sh
    bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3
    rm ~/miniconda3/miniconda.sh

    ~/miniconda3/bin/conda init bash

    conda install -n base conda-libmamba-solver
    conda config --set solver libmamba

    conda config --add channels conda-forge
    conda config --set channel_priority strict
fi
