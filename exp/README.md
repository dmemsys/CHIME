# Reproduce All Experiment Results

In this folder, we provide code and scripts for reproducing figures in our paper. A 10-node r650 cluster is needed to reproduce all the results.
The name of each script corresponds to the number of each figure in our paper.

> [!TIP]  
> **📢 Since some of the scripts in this directory take a long execution time (as we mark below), we strongly recommend you create a *tmux session* (using the command `tmux`) on each node to avoid script interruption due to network instability.**


## Full YCSB workloads

In our paper, we use YCSB workloads, most of which includes 60 million entries and operations. You should generate full YCSB workloads on **all** nodes.

> [!NOTE]  
> **📢 If you are using our created cluster, you can just copy our generated workloads to save time, using the following commands:**
```shell
sudo su
cd CHIME/ycsb
rm -rf workloads
# This takes about 0.5 minutes
cp -r /users/xuchuan/workloads workloads
```
Otherwise, if you want to generate the workloads by yourself, using the following commands:
```shell
sudo su
cd CHIME/ycsb
rm -rf workloads
# This takes about 1 hours and 28 minutes. Let it run and you can do your own things.
sh generate_full_workloads.sh
```


## Source Code of Baselines

In our paper, we compare CHIME to the existing three types of DM range indexes: **SMART** (radix tree), **Sherman/Marlin** (B+ tree), and **ROLEX** (learned index).

* SMART (*OSDI'23*) is the latest radix tree on DM. We directly use [its open-source repo](https://github.com/dmemsys/SMART).

* Sherman (*SIGMOG'22*) is a classic B+ tree on DM. We enhance it with *two-level cache line versions* to fix [its known bugs](https://github.com/thustorage/Sherman?tab=readme-ov-file#known-bugs). Since CHIME is designed based on Sherman, the source codes of Sherman have been included inside this repo.

* Marlin (*ICPP'23*) is the latest B+ tree on DM designed for variable-length values. We [re-implement it](https://github.com/River861/Marlin) since it is not open-source.

* ROLEX (*FAST'23*) is the latest learned index on DM. We [re-implement it](https://github.com/River861/ROLEX) with [its open-source models](https://github.com/iotlpf/ROLEX).


Use the following command to clone the required baselines in the same path of CHIME (*i.e.*, the home directory) on **all** nodes:
```shell
git clone https://github.com/dmemsys/SMART
git clone https://github.com/River861/Marlin
git clone https://github.com/River861/ROLEX
```


## Additional Setup

> [!NOTE]  
> **📢 Just skip this section if you are using our created cluster, where the default setup is feasible.**

* Change the `home_dir` value in `./params/common.json` to your actual home directory path (*e.g.*, /users/TempUser).
    ```json
    "home_dir" : "/your/home/directory"
    ```

* Change the `workloads_dir` value in `./params/common.json` to the actual path of the generated YCSB workloads (*e.g.*, /users/TempUser/CHIME/ycsb/workloads).
    ```json
    "workloads_dir" : "/path/to/workloads"
    ```

* Change the `master_ip` value in `./params/common.json` to the IP address of a master node of the r650 cluster. In the r650 cluster,  we define a node which can directly establish SSH connections to other nodes as a **master** node.
    ```json
    "master_ip": "10.10.1.2"
    ```


## Start to Run

> [!important]  
> **📢 All the scripts only need to be run on the **master** node.**  
> **(If you are using our created cluster, the master node is the node with the IP "10.10.1.2" and the host name "node-0")**

* You can run all the scripts with a single batch script using the following command:
    ```shell
    sudo su
    cd CHIME/exp
    # This takes about 16 hours and 45 minutes. Let it run and just check the results in the next day.
    sh run_all.sh
    ```
* Or, you can choose to run the scripts one by one, or run some specific scripts you like:
    ```shell
    sudo su
    cd CHIME/exp
    # This takes about 23 minutes
    python3 fig_03a.py
    # This takes about 25 minutes
    python3 fig_03b.py
    # This takes about 25 minutes
    python3 fig_03c.py
    # This takes about 2 minutes
    python3 fig_03d.py
    # This takes about 12 minutes
    python3 fig_04a.py
    # This takes about 12 minutes
    python3 fig_04b.py
    # This takes about 16 minutes
    python3 fig_04c.py
    # This takes about 7 hours and 37 minutes
    python3 fig_12.py
    # This takes about 50 minutes
    python3 fig_13.py
    # This takes about 35 minutes
    python3 fig_14.py
    # This takes about 44 minutes
    python3 fig_15a.py
    # This takes about 40 minutes
    python3 fig_15b.py
    # This takes about 5 minutes
    python3 fig_16.py
    # This takes about 34 minutes
    python3 fig_17.py
    # This takes about 18 minutes
    python3 fig_18a.py
    # This takes about 45 minutes
    python3 fig_18b.py
    # This takes about 48 minutes
    python3 fig_18c.py
    # This takes about 37 minutes
    python3 fig_18d.py
    # This takes about 31 minutes
    python3 fig_18e.py
    # This takes about 10 minutes
    python3 fig_18f_19b.py
    # This takes about 17 minutes
    python3 fig_19a.py
    # This takes about 8 minutes
    python3 fig_19c.py
    ```

* The json results and PDF figures of each scirpt will be stored inside a new directoty `./results`.

    The results you get may not be exactly the same as the ones shown in the paper due to changes of physical machines.
    And some curves may fluctuate due to the instability of RNICs in the cluster.
    However, all results here support the conclusions we made in the paper.

    Please save the results and figures to your own laptop/PC after running all experiments in case other AE reviewers execute the scripts and overwrite your results.
