# CHIME: A Cache-Efficient and High-Performance Hybrid Index on Disaggregated Memory


This is the implementation repository of our *SOSP'24* paper: **CHIME: A Cache-Efficient and High-Performance Hybrid Index on Disaggregated Memory**.
This artifact provides the source code of **CHIME** and scripts to reproduce all the experiment results in our paper.
**CHIME**, a <u>**C**</u>ache-efficient and <u>**H**</u>igh-performance Hybrid <u>**I**</u>ndex on disaggregated <u>**ME**</u>mory, combines the B+ tree with hopscotch hashing, to break the trade-off between cache consumption and read amplifications for existing DM range indexes.


- [CHIME](#chime-a-cache-efficient-and-high-performance-hybrid-index-on-disaggregated-memory)
  * [Supported Platform](#supported-platform)
  * [Create Cluster](#create-cluster)
  * [Source Code *(Artifacts Available)*](#source-code-artifacts-available)
  * [Environment Setup](#environment-setup)
  * [YCSB Workloads](#ycsb-workloads)
  * [Getting Started *(Artifacts Functional)*](#getting-started-artifacts-functional)
  * [Reproduce All Experiment Results *(Results Reproduced)*](#reproduce-all-experiment-results-results-reproduced)
  * [Paper](#paper)
  * [Acknowledgments](#acknowledgments)


## Supported Platform
We strongly recommend you to run CHIME using the r650 instances on [CloudLab](https://www.cloudlab.us/) as the code has been thoroughly tested there.
We haven't done any test in other hardware environment.

If you want to reproduce the results in the paper, 10 r650 machines are needed; otherwise, fewer machines (i.e., 3) is OK. Each r650 machine has two 36-core Intel Xeon CPUs, 256 GB of DRAM, and one 100 Gbps Mellanox ConnectX-6 RNIC. Each RNIC is connected to a 100 Gbps Ethernet switch.

## Create Cluster

Follow the steps below to create an experimental cluster with 10 r650 nodes on CloudLab:

1) Log into your own account.

2) Click `Experiments`|-->`Create Experiment Profile`. Upload `./script/cloudlab.profile` provided in this repo.
Input a file name (*e.g.*, CHIME) and click `Create` to generate the experiment profile for CHIME.

3) Click `Instantiate` to create a 10-node cluster using the profile (This takes about 7 minutes).

4) Try logging into and check each node using the SSH commands provided in the `List View` on CloudLab. If you find some nodes have broken shells, you can reload them via `List View`|-->`Reload Selected`.


## Source Code *(Artifacts Available)*
Now you can log into all the CloudLab nodes. Using the following command to clone this github repo in the home directory (*e.g.*, /users/TempUser) of **all** nodes:
```shell
git clone https://github.com/dmemsys/CHIME.git
```


## Environment Setup

You have to install the necessary dependencies in order to build CHIME.
Note that you should run the following steps on **all** nodes you have created.

1) Set bash as the default shell. And enter the CHIME directory.
    ```shell
    sudo su
    chsh -s /bin/bash
    cd CHIME
    ```

2) Install Mellanox OFED.
    ```shell
    # It doesn't matter to see "Failed to update Firmware"
    # This takes about 8 minutes
    sh ./script/installMLNX.sh
    ```

3) Resize disk partition.

    Since the r650 nodes remain a large unallocated disk partition by default, you should resize the disk partition using the following command:
    ```shell
    # It doesn't matter to see "Failed to remove partition" or "Failed to update system information"
    sh ./script/resizePartition.sh
    # This takes about 6 minutes
    reboot
    # After rebooting, log into all nodes again and execute:
    sudo su
    resize2fs /dev/sda1
    ```

4) Enter the directory. Install the required libraries.
    ```shell
    cd CHIME
    # This takes about 3 minutes
    sh ./script/installLibs.sh
    ```


## YCSB Workloads

You should run the following steps on **all** nodes.

1) Download YCSB source code.
    ```shell
    sudo su
    cd CHIME/ycsb
    curl -O --location https://github.com/brianfrankcooper/YCSB/releases/download/0.11.0/ycsb-0.11.0.tar.gz
    tar xfvz ycsb-0.11.0.tar.gz
    mv ycsb-0.11.0 YCSB
    ```

2) We first generate a small set of YCSB workloads here for a **quick start** (i.e, kick-the-tires).
    ```shell
    # This takes about 20 seconds
    sh generate_small_workloads.sh
    ```


## Getting Started *(Artifacts Functional)*

* Hugepages setting on **all** nodes (This step should be re-execute every time you reboot the nodes).
    ```shell
    sudo su
    echo 36864 > /proc/sys/vm/nr_hugepages
    ulimit -l unlimited
    ```

* Return to the root directory of CHIME and execute the following commands on **all** nodes to compile CHIME:
    ```shell
    mkdir build; cd build; cmake ..; make -j
    ```

* Execute the following command on **one** node to initialize the memcached:
    ```shell
    /bin/bash ../script/restartMemc.sh
    ```

* Execute the following command on **all** nodes to split the workloads:
    ```shell
    python3 ../ycsb/split_workload.py <workload_name> <key_type> <CN_num> <client_num_per_CN>
    ```
    * workload_name: the name of the workload to test (*e.g.*, `a` / `b` / `c` / `d` / `e`).
    * key_type: the type of key to test (*i.e.*, `randint`).
    * CN_num: the number of CNs.
    * client_num_per_CN: the number of clients on each CN.

    **Example**:
    ```shell
    python3 ../ycsb/split_workload.py a randint 10 24
    ```

* Execute the following command on **all** nodes to conduct a YCSB evaluation:
    ```shell
    ./ycsb_test <CN_num> <client_num_per_CN> <coro_num_per_client> <key_type> <workload_name>
    ```
    * coro_num_per_client: the number of coroutine in each client (2 is recommended).

    **Example**:
    ```shell
    ./ycsb_test 10 24 2 randint a
    ```

* Results:
    * Throughput: the throughput of **CHIME** of the whole cluster will be shown in the terminal of the first node (with 10 epoches by default).
    * Latency: execute the following command on **one** node to calculate the latency results of the whole cluster:
        ```shell
        python3 ../us_lat/cluster_latency.py <CN_num> <epoch_start> <epoch_num>
        ```

        **Example**:
        ```shell
        python3 ../us_lat/cluster_latency.py 10 1 10
        ```


## Reproduce All Experiment Results *(Results Reproduced)*
We provide code and scripts in `./exp` folder for reproducing our experiments. For more details, see [./exp/README.md](./exp#reproduce-all-experiment-results).


## Paper
If you use CHIME in your research, please cite our paper:
```bibtex
@inproceedings{chime2024,
  author    = {Xuchuan Luo and Jiacheng Shen and Pengfei Zuo and Xin Wang and Michael R. Lyu and Yangfan Zhou},
  title     = {{CHIME}: A Cache-Efficient and High-Performance Hybrid Index on Disaggregated Memory},
  booktitle = {Proceedings of the 30th Symposium on Operating Systems Principles ({SOSP} 2024)},
  year      = {2024},
  address   = {Austin, TX},
  pages     = {110--126},
  url       = {https://doi.org/10.1145/3694715.3695959},
  publisher = {{ACM}},
  month     = nov,
}
```

## Acknowledgments
This repository adopts [Sherman](https://github.com/thustorage/Sherman)'s codebase and [SMART](https://github.com/dmemsys/SMART)'s RDWC technique. We really appreciate it.
