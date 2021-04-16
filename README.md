# GraphColoring

Solution to the graph coloring problem using a Pregel-like algotithm with Apache Spark 

Environment setup to run the script: 
install virtual-box and vagrant
run vagrant up 
vagrant ssh
~/spark-2.4.7-bin-hadoop2.7/bin/spark-submit --packages graphframes:graphframes:0.7.0-spark2.3-s_2.11  /vagrant/graph_coloring.py

