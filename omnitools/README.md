install
-------

```sh
# read the contents of this file before running it
# you may want to skip this step after reading it and install manually
sh install.sh
```

setting proxy
-------------

```sh
# part of install.sh, however can be run separately, if needed
# the ~/.docker/config.json file will be created / overwritten based on
# HTTP_PROXY, HTTPS_PROXY, FTP_PROXY, NO_PROXY environmental variables
# will be skipped if none are set
# HTTP_PROXY will be used when running `omnitools docker build` command
sh proxy/set_docker_proxy.sh
```

commands
--------

```sh
omnitools
----------------------------------------------------------------------
help                 display all options
info                 display omnitools information for current working directory
lint                 run all available linters
test                 run all tests associated with this repo
docker               perform project-relevant docker commands
```

customise and extend
--------------------

One of the core principles of omnitools is to allow the user to customise and
extend it's functionality. To do this, create a `.omnitools` folder *in your
project root* to place your desired customisations.


### custom configuration

One of the main ways to define customisations is with a `config.sh` file placed
into a `.omnitools` directory of your project root. Here you can set
project-specific configurations for your project. For example:

`.omnitools/config.sh`
```sh
#!/usr/bin/env sh
# shellcheck disable=SC2034

set -e


# just some custom hash example - all of this is totally up to your needs
hash() {
  if command -v md5 > /dev/null; then
    find "$1" -type f -and -not -path "./.git/*" -exec md5 -q {} \; | md5
  elif command -v md5sum > /dev/null; then
    find "$1" -type f -and -not -path "./.git/*" -exec md5sum {} \; | awk '{ print $1 }' | md5sum | awk '{ print $1 }'
  else
    >&2 echo "[error] failed to hash. no md5 or md5sum found"
    exit 1
  fi
}

omnitools_IMAGE=mycustomtoolbox:0.1
# DOCKER_COMPOSE_FILE=
DOCKER_TAG="myproject:$(hash "$PROJECT_ROOT")"
# DOCKER_FILE=
# DOCKER_CONTEXT=
# DOCKER_PROGRESS=
# HOME_DIR=
```

This file will be automatically picked up and when you run `omnitools docker build`
given the config.sh file above it will build your project as the image
"myproject" and a tag generated from an md5 hash of your files.

### custom functions

Adding custom functions to omnitools is trivial. Place a shell file into your
projects `.omnitools/cmds/` directory with the filename to match the desired
command. In that file you should place a comment starting `# help: ` under the
shebang indicating the help text you wish to display. For example, lets say
we wanted a "frog" function, we would add:

`./.omnitools/cmds/frog.sh`
```sh
#!/usr/bin/env sh
# help: ribbit im a frog

echo "ribbit! ...ribbit!"
```

Then when we run `omnitools` we can see our new command listed:

```sh
omnitools
----------------------------------------------------------------------
help                 display all options
info                 display omnitools information for current working directory
lint                 run all available linters
test                 run all tests associated with this repo
docker               perform project-relevant docker commands
frog                 ribbit im a frog
```

Now when we run `omnitools frog` we get to see our wonderful script in action:

```sh
omnitools frog
ribbit! ...ribbit!
```

Now if we wanted to add some custom flags to our frog function we can add
them to the help description with an `# flags:` section like so:

```sh
#!/usr/bin/env sh
# help: ribbit im a frog
# flags:
# --jump | optional. tells our frog to jump
# --croak | optional. tells our frog to croak

FLAG_croak="${FLAG_croak:-False}"
FLAG_jump="${FLAG_jump:-False}"

if [ "$FLAG_croak" = "True" ]; then
  echo "ribbit! ...ribbit!"
fi
if [ "$FLAG_jump" = "True" ]; then
  echo "...jump!"
fi
```

With our updated file we can make use of our new flags:

```sh
# our flags are now visible in the help section
$ omnitools frog --help
omnitools frog
  --help             get all available help options
  --jump             optional. tells our frog to jump
  --croak            optional. tells our frog to croak

# and they're ready to use
$ omnitools frog --jump --croak
ribbit! ...ribbit!
...jump!
```

If our custom file has a main() method defined it will be executed by default
when the command is requested. For example, with new new main() method:

```sh
#!/usr/bin/env sh
# help: ribbit im a frog
# flags:
# --jump | optional. tells our frog to jump
# --croak | optional. tells our frog to croak

FLAG_croak="${FLAG_croak:-False}"
FLAG_jump="${FLAG_jump:-False}"

if [ "$FLAG_croak" = "True" ]; then
  echo "ribbit! ...ribbit!"
fi
if [ "$FLAG_jump" = "True" ]; then
  echo "...jump!"
fi

main() {
  echo "im fired automatically"
}
```

When we fire the previous command we now get:

```sh
$ omnitools frog --jump --croak
ribbit! ...ribbit!
...jump!
im fired automatically
```

__note:__ all logic should be contained within the main() method - in the
example above we handle flags outside of main(). This makes the script less
friendly to being included from other scripts. The above was just for example
purposes


### custom toolbox image

Want to use a handrolled docker image for running all of your linting / testing
etc in? Great. Set `omnitools_IMAGE=yourimage:0.1` in your `.omnitools/config.sh`. The
only requirement is that `omnitools` is installed in your image. The default image
is pemcconnell/omnitools:latest and it's found in ./toolboxes/Dockerfile.python in
this repo.

### overriding functions

Using the same approach that we took for custom functions, we can overwrite
core commands in the same way. For example, lets say that we want to change
`omnitools lint` so that it prints "lgtm" every time someone runs it, we create
a file in our project at `./.omnitools/cmds/lint.sh`:

```sh
#!/usr/bin/env sh
# help: linting made easy

echo "lgtm"
```

Now when you run `omnitools` you will see the core `lint` command has been replaced
by the custom one that we have created:

```sh
omnitools
----------------------------------------------------------------------
help                 display all options
info                 display omnitools information for current working directory
frog                 ribbit im a frog
lint                 linting made easy
test                 run all tests associated with this repo
docker               perform project-relevant docker commands
```

When we run `omnitools lint` we see our script has been executed:

```sh
omnitools lint
lgtm
```

command flags
-------------

You can get access to long flags (omnitools commmand --something --else=xx) for
free. These are interpreted in omnitools.sh and eval'ed to FLAG_$name. Where a
value is not declared, True is set. For example:

```sh
omnitools docker build --tag=blopeur/omnitools:latest
# variable $FLAG_tag equals blopeur/omnitools:latest

omnitools docker build --foo
# variable $FLAG_foo equals True
```

core commands
-------------

### linting

Running `omnitools lint` will run the linter against your current working


### testing

Running `omnitools test` will run the tester against your current working directory.


### docker build

To build with docker you can run `omnitools docker build`. This will attempt to autodetect if it is a docker-compose or Dockerfile build. If Dockerfile, it will attempt to detect if it is a buildkit image. An example (buildkit):


### docker run

Once the image has been built you can run the container using `omnitools docker run`. This will automatically mount in a series of common volumes. For convenience this generated 'docker run' command is pasted prior to running the container so you can easily copy/paste it and tweak as required:

```sh
 omnitools docker run
 [ info      ] running docker run --rm  -e DISPLAY=unix/private/tmp/com.apple.launchd.WJTehPPKq2/org.macosforge.xquartz:0 -e HOST_HOME=/Users/someguy -ti omnitools:05b8266beeaaf11979f6ef888ad40db5
root@de84379554b6:/#
```

### building the base image 

If you want to build the base image yourself you can type the following command: 

```sh
docker build -t="blopeur/omnitools:latest" -f omnitools/toolboxes/mega.Dockerfile .
```

### docker bash command line

In order to get inside the docker container command line, run

```sh
omnitools bash
```

If the image name is different from "blopeur/omnitools:latest", supply it with the corresponding parameter

```sh
OMNITOOLS_IMAGE=your_image_name omnitools bash
```

### compiling omni-runtime

```sh
omnitools build-omni
```

All the output will appear in the local "build" folder. By default, both cpp and java codes are compiled. To select one of those, use flags: --cpp, --java. cpp code is compiled in release mode. To change that, use one of the debug flags. For example, building only cpp code with low debug level would result in the following command (use --help to see all the options):

```sh
omnitools build-omni --cpp --debug_low
```

If more detailed output is needed during the script execution, use the DEBUG command option:

```sh
DEBUG=true omnitools build-omni
```

### testing omni-runtime

```sh
omnitools test-omni
```

Testing also can be done separately for cpp and java parts, use flags --cpp and --java. If no flags are specified, by default testing will be done for both parts.


### packaging omni-runtime

```sh
omnitools package-omni
```

Packaging is done for the cpp part of the code in .deb and .rpm files. By default both are produced, however each can be selected using --deb or --rpm flags. The output will also be located in the "build" folder.


### cleaning omni-runtime

```sh
omnitools clean-omni
```

This command will remove the whole "build" folder, so use it with caution.


