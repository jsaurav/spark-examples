// Chapter 18 Docs - Monitoring and Debugging

// Things which can be monitored.
// 1. Spark Application and Jobs - Monitor SPARK UI and logs.
// 2. JVMs - jstack , jmap , jconsole
// 3. OS
// 4. Cluster - Ganglia & Prometheus are cluster monitoring tools.

// There are 2 main things you want to monitor in a spark applicaiton.
// 1. The processes running your applicaiton (cpu & memory usage).
//    Spark has a configurable metrics system based on the Drowizard Metrics Library.	

// 2. query execution inside it (jobs and tasks). It includes queries, jobs , stages and tasks.

// Spark Logs and Spark UI

