start_server {} {
    set i [r info]
    regexp {Arana/Kiwi_version:(.*?)\r\n} $i - version
    regexp {Arana/Kiwi_git_sha:(.*?)\r\n} $i - sha1
    puts "Testing Arana/Kiwi version $version ($sha1)"
}
