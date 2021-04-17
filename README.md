# GraphColoring

Solution to the Graph Coloring problem using a Pregel-like algorithm in Apache Spark 

Setup and execution 
* install _virtual-box_ and _vagrant_
* create and run VM: _vagrant up_ in Vagrantfile's directory
* open SSH connection with the VM: _vagrant ssh_
* script execution: _~/spark-2.4.7-bin-hadoop2.7/bin/spark-submit --packages graphframes:graphframes:0.7.0-spark2.3-s_2.11  /vagrant/graph_coloring.py_

