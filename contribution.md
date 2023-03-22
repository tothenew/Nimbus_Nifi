
# Contributing


Contributions are always welcome, no matter how large or small. Before contributing, please read the code of conduct.


## Types of Contributions


### Report Bugs

If you find a bug in the source code, you can help us by submitting an issue or, even better, a pull request with a fix.


If you are reporting a bug, please include:

* Your environment details.
* Detailed steps to reproduce the bug.

### Fix Bugs


Look through the GitHub issues for bugs. Anything tagged with "bug"
and "help wanted" is open to whoever wants to implement it.


### Request a new feature
If you think of a new feature that would make Nimbus better for everyone, please start a discussion to propose the idea.

* Explain in detail how it would work.
* Keep the scope as narrow as possible, to make it easier to implement.



## Get Started!

### Github Workflow

Ready to contribute? Here's how to set up Nimbus for local development.

1. Fork the Nimbus repository.

    github https://github.com/tothenew/nimbus-nifi.git

2. Clone your fork locally.

    ```
    git clone git@github.com:<username>/nimbus-nifi.git
    ```

    replace `<username>` with your GitHub username.

3. Add a remote to keep up with upstream changes

    ```
    git remote add upstream https://github.com/tothenew/nimbus-nifi.git
    ```

    If you already have a copy, fetch upstream changes

    ```
    git fetch upstream master
    ```

4. Create a feature branch for local development.

    ```
    git checkout -b <Name_of_feature_or_Bug> remotes/upstream/master
    ```


5. Submit a pull-request

    ```
    git commit -m <meaningfull msg describing your changes> 
    git push origin <Name_of_feature_or_Bug>
   ```

    Go to your Nimbus fork main page

    ```
    https://github.com/<username>/nimbus-nifi
    ```

    If you recently pushed your changes GitHub will automatically pop up a
    `Compare & pull request` button for any branches you recently pushed to. If you
    click that button it will automatically offer you to submit your pull-request
    to the Nimbus repository.

    - Give your pull-request a meaningful title.
    - In the description, explain your changes and the problem they are solving.

For the installation and Usage, please visit the [README](README.md).

## Pull Request Guidelines


Before you submit a pull request, check that it meets these guidelines:

1. Make sure your code respects existing formatting conventions.
2. If the pull request adds functionality, the docs should be updated. Put
   your new functionality into a function with a docstring.
3. Bugfixes / New Features should include a unit test.
