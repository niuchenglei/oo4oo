

def classpath_jars():
    """Returns a list with the  paths to the required jar files.

    The sagemakerpyspark library is mostly a wrapper of the scala sagemakerspark sdk and it
    depends on a set of jar files to work correctly. This function retrieves the location
    of these jars in the local installation.

    Returns:
        List of absolute paths.
    """
    import pkg_resources
    pkg_dir = __name__
    jars_dir = "jars/"

    #bundled_jars = pkg_resources.resource_listdir(pkg_dir, jars_dir)
    #jars = [pkg_resources.resource_filename(pkg_dir, jars_dir + jar) for jar in bundled_jars]
    jars = ["/Users/chenglei/nuclear/mleap-oo4oo/scala/target/scala-2.12/mleap-oo4oo_2.12-0.0.1.jar", "/Users/chenglei/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/ml/combust/mleap/mleap-spark-extension_2.12/0.23.0/mleap-spark-extension_2.12-0.23.0.jar", "/Users/chenglei/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/spark/spark-mllib_2.12/3.2.2/spark-mllib_2.12-3.2.2.jar", "/Users/chenglei/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/spray/spray-json_2.12/1.3.2/spray-json_2.12-1.3.2.jar", "/Users/chenglei/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/jsuereth/scala-arm_2.12/2.0/scala-arm_2.12-2.0.jar", "/Users/chenglei/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/ml/combust/bundle/bundle-hdfs_2.12/0.23.0/bundle-hdfs_2.12-0.23.0.jar", "/Users/chenglei/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/typesafe/config/1.3.0/config-1.3.0.jar", "/Users/chenglei/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/thesamet/scalapb/scalapb-runtime_2.12/0.11.1/scalapb-runtime_2.12-0.11.1.jar", "/Users/chenglei/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/ml/combust/bundle/bundle-ml_2.12/0.23.0/bundle-ml_2.12-0.23.0.jar", "/Users/chenglei/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/ml/combust/mleap/mleap-spark-base_2.12/0.23.0/mleap-spark-base_2.12-0.23.0.jar", "/Users/chenglei/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/ml/combust/mleap/mleap-runtime_2.12/0.23.0/mleap-runtime_2.12-0.23.0.jar", "/Users/chenglei/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/ml/combust/mleap/mleap-core_2.12/0.23.0/mleap-core_2.12-0.23.0.jar"]

    return jars


__all__ = ['classpath_jars']