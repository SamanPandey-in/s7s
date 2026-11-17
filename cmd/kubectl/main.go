package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"sigs.k8s.io/yaml"

	"s7s/pkg/api"
	"s7s/pkg/client"
)

const usage = `usage:
	kubectl apply -f FILE
	kubectl get pods [NAME]
	kubectl delete pod NAME

flags (any subcommand):
	-n NAMESPACE default "default"
	-s URL default $KUBE_SERVER or http://localhost:8080
`

func main() {
	// here os.Args is a slice of strings containing the command-line arguments passed to the program
	// eg: "go run main.go get pods -n dev" so conceptually: os.Args = []string{"main.go", "get", "pods", "-n", "dev"}
	// so always
	// os.Args[0] = program name, [1] = first argument, [2:] = remaining argument
	//
	// so here: if someone runs "s7s" or "go run main.go", there is no command, thus we print the usage instructions to standard error
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}

	cmd, args := os.Args[1], os.Args[2:]

	// now this is a command router, which decides which function should handle the request
	var err error
	switch cmd {
	case "apply":
		err = runApply(args)
	case "get":
		err = runGet(args)
	case "delete":
		err = runDelete(args)
	default:
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	// if something fails, print error, exit with status code 1
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// centralizes common flags so user doesnt has to redefine namespace and server configs in every command
func newFlagSet(name string) (fs *flag.FlagSet, ns, server *string) {
	fs = flag.NewFlagSet(name, flag.ExitOnError)
	ns = fs.String("n", "default", "namespace")
	server = fs.String("s", envOr("KUBE_SERVER", "http://localhost:8080"), "API server URL")
	return
}

// parse lets flags appear before or after positional arguments
// so both "get pods -n dev" and "get -n dev pods" work.
// WHY?
// because flag package has a limitation: it stops parsing flags when it encounters the first non-flag argument
// so here we implement a workaround
func parse(fs *flag.FlagSet, args []string) []string {
	var pos []string
	// creates a slice that will hold positional arguments
	// Positional arguments are arguments that arent flags
	// eg> kubectl get pods nginx -n dev
	// positional arguments: []string{"pods", "nginx"}

	for {
		fs.Parse(args)

		// fs.NArg() returns the number of non-flag arguments remaining after parsing
		if fs.NArg() == 0 {
			return pos
		}

		// extract the first pos arg
		pos = append(pos, fs.Arg(0))
		// remove it
		args = fs.Args()[1:]
	}
	/*
		 * What happens when we run parse(fs, args)
			* kubectl apply -f pod.yaml -n dev
			* after parsing:
			* *file == "pod.yaml"
			* *ns == "dev"
			* *server == "http://localhost:8080"
	*/
}

/*
 * NOTES:
 * This implementation works for current simple commands, but it's not a fully general-purpose CLI parser.
 * Also, errors returned by fs.Parse() are ignored here. Because we use flag.ExitOnError in newFlagSet,
 * parsing errors terminates the process, but the function itself isnt handling them yet.
 */

/* KUBERNETES PART */
// Applying a pod manifest
func runApply(args []string) error {
	// this function handles `kubectl apply -f pod.yaml`
	// Its responsibility is to read a YAML file and create or update a Pod through the API server

	fs, ns, server := newFlagSet("apply")       // creates a FlagSet and registers -n and -s
	file := fs.String("f", "", "manifest file") // registers a new flag -f
	// now -f pod.yaml means:: manifest file is pod.yaml
	// its default value is an empty string

	parse(fs, args) // this parses all flags
	// the return value is ignored here because this command doesnt need the positional args

	if *file == "" {
		return errors.New("-f FILE is required")
	} // SRP: this fn doesnt print the error, it returns the error to main() and it is the actor who handles printing

	data, err := os.ReadFile(*file) // see notes pt.1
	if err != nil {
		return err
	}

	// Now, convert that YAML into Go struct
	var pod api.Pod

	if err := yaml.Unmarshal(data, &pod); err != nil { // see notes pt.2
		return fmt.Errorf("parse %s: %w", *file, err) // creates a new error with additional context
	}

	// Validate resource kind
	if pod.Kind != "Pod" {
		return fmt.Errorf("unsupported kind %q (only Pod for now)", pod.Kind)
	}

	// creating an api client
	c := client.New(*server)

	// check whether the Pod already exists
	_, err = c.GetPod(*ns, pod.Metadata.Name) // if pod exists, err == nil

	// decide bw create and update
	switch {
	case client.IsNotFound(err):
		if _, err := c.CreatePod(*ns, &pod); err != nil {
			return err
		}
		fmt.Printf("pod/%s created\n", pod.Metadata.Name)
	case err != nil:
		return err
	default:
		if _, err := c.UpdatePod(*ns, &pod); err != nil {
			return err
		}
		fmt.Printf("pod/%s configured\n", pod.Metadata.Name)
	} // see notes pt 3
	return nil
}

/*
 * NOTES:
 * 1. os.ReadFile() reads the entire file and returns []byte
 * WHY []byte instead of string?
 * because file contents are raw bytes. YAML parsing libraries can directly process byte slices.
 * so data contains the bytes representing the YAML document, if the file doesnt exists or cannot be read, it returns the error
 *
 * 2. yaml.Unmarshal(data, &pod) converts YAML bytes into go struct.
 * The &pod means the address of the variable
 * why pass a pointer? -> Because Unmarshal needs to modify the original variable
 *
 * 3. our implmentation follows a basic idea of k8s apply:
 * if already exists -> replace, if resource doesnt exists -> create a new pod
 * This is a very simplified version of it.
 * REAL k8s kubectl apply involves declarative configuration and field ownership/merging
 * Also the GetPod() and followed by CreatePod() has a possible race condition: another client could create the same Pod between those two requests.
 */

func runGet(args []string) error {
	fs, ns, server := newFlagSet("get")
	pos := parse(fs, args)
	if len(pos) < 1 || !isPod(pos[0]) {
		return errors.New("usage: kubectl get pods [NAME]")
	}
	c := client.New(*server)

	var pods []api.Pod
	if len(pos) > 1 {
		p, err := c.GetPod(*ns, pos[1])
		if err != nil {
			return err
		}
		pods = []api.Pod{*p}
	} else {
		list, err := c.ListPods(*ns)
		if err != nil {
			return err
		}
		pods = list.Items
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 8, 3, ' ', 0)
	fmt.Fprintln(w, "NAME\tSTATUS\tNODE\tAGE")
	for _, p := range pods {
		node := p.Spec.NodeName
		if node == "" {
			node = "<none>"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", p.Metadata.Name, p.Status.Phase, node, age(p.Metadata.CreationTimestamp))
	}
	return w.Flush()
}

func runDelete(args []string) error {
	fs, ns, server := newFlagSet("delete")
	pos := parse(fs, args)
	if len(pos) != 2 || !isPod(pos[0]) {
		return errors.New("usage: kubectl delete pod NAME")
	}

	if err := client.New(*server).DeletePod(*ns, pos[1]); err != nil {
		return err
	}
	fmt.Printf("pod/%s deleted \n", pos[1])
	return nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func isPod(s string) bool { return s == "pod" || s == "pods" }

func age(t time.Time) string {
	d := time.Since(t).Round(time.Second)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	default:
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
}
