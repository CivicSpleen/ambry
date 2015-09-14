.. _documenting_bundles:

While most of the documentation for a dataset is provided by the dataset source, there is also documentation that the bundle author can add to the bundle. Bundle documentation is added in three places:

- The `about.summary` metadata hold a short overview of the dataset
- The :file:`meta/documentation.md` file provides extra documentation in markdown format
- The :file:`meta/README.md.template` is the source for the generated README.md file. 

Generally, the :file:`meta/documentation.md` is the place to put information about the dataset that is not covered in an external document, such as errors covered in conversion, modificatons made to the dataset in conversion, or information that was provided by the source in email, for which there is no URL. 

Information relevant to the bundle author or maintainer should be placed in :file:`meta/README.md.template`. This file has python format interpolations that get expanded to metadata values. The default file just has the bundle title and summary, and at the end of the ``prepare`` phase, the file is interpolated and copied into :file:`README.md`, where it is avaialble for display as the Github README. 