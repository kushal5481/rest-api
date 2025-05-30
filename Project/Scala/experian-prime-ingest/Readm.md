# Data Access

Experian-Prime-Ingest is Experian’s software framework for high throughput and high-frequency event processing. Experian-Prime-Ingest Core has libraries needed for most Experian-Prime-Ingest modules.

## How to use Experian-Prime-Ingest

In leveraging the Experian-Prime-Ingest framework, extension developers can focus on business rules implementations and allow the framework to handle the technical aspect like execution, scalability and reliability thereby enabling developers to rapidly and easily develop and deploy high-performance high-throughput and high-frequency data pipelines.

## Important Commands

From Root Directory = mvn clean package -pl .\experian-prime-ingest-core\ -am

From Root Directory = spark-submit --class com.experian.ind.nextgen.fileinvestigation.FileInvestigator --master local[*] .\experian-prime-ingest-core\target\experian-prime-ingest-core-1.0-SNAPSHOT-jar-with-dependencies.jar

## Running Experian-Prime-Ingest / Installation

### Technologies

* Spark
* Hadoop
* Scala
* Java
If you run into any issues, file an issue on Github or ask us on Teams (see above).

### Building manually
Clone repo
Run App. For more details can be found [here] (https://pages.experian.com/display/ODC/Experian-Prime-Ingest+Development+Center).

## Contributing

PRs, bug reports, and feature requests are welcome! For major changes, please open an issue first
to discuss what you would like to change.
Please make sure to update tests as appropriate.Our guidelines for contributing are explained in [CONTRIBUTING.md](CONTRIBUTING.md).
It contains useful information on our coding standards, testing, documentation, how
we use git and GitHub and how to get your code reviewed.

## License

[Experian](https://www.experian.com/)
