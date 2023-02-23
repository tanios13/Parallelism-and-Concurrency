<!-- ABOUT THE PROJECT -->
## About The Project

Labs to learn Parallelism and concurrency

## What I learned from this project
* Learn about parallel computing: task/data parallelism
* Learn concurrency: atomic operations, monitors, locks, deadlocks

<!-- GETTING STARTED -->
## Getting Started

1. Let’s upgrade the IDE support first, close VSCode if it’s open and run:
   ```sh
   code --force --install-extension scalameta.metals
   ```

2. Clone the repo
   ```sh
   git clone https://github.com/tanios13/Parallelism-and-Concurrency.git
   ```

3. In the terminal of the desired lab, run
   ```sh
   sbt
   ```

   To start the SBT build tool and loads your project's configuration from the `build.sbt` file in the project directory.

4. Then to test the code, run
   ```sh
   test
   ```