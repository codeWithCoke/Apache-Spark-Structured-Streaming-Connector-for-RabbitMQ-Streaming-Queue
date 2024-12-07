package com.customStreamingSource.rabbitMQUtils

object PathUtil {
  def convertToSystemPath(path: String): String = {
    // Determine the current operating system
    val os = System.getProperty("os.name").toLowerCase
    val isWindows = os.startsWith("windows")
    // Normalize path for the operating system
    var finalPath: String = path
    if (isWindows) {
      finalPath = finalPath.replace('/', '\\') // Ensure backslashes for Windows

      if (finalPath.startsWith("\\") && finalPath.length > 2 && finalPath.charAt(2) == ':') {
        // Strip leading slash in front of Windows drive letter
        finalPath = finalPath.substring(1)
      }
    }
    else finalPath = finalPath.replace('\\', '/') // Ensure forward slashes for Linux
    finalPath
  }

}
