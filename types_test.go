package wsrpc_test

import (
	"os/exec"
	"strings"
	"testing"
)

// These compile actual invalid consumers against the public package.
func TestTypeContractsRejectInvalidConsumers(t *testing.T) {
	for _, name := range []string{"arguments", "handler", "result"} {
		t.Run(name, func(t *testing.T) {
			cmd := exec.Command("go", "test", "./testdata/invalid/"+name)
			output, err := cmd.CombinedOutput()
			if err == nil || !strings.Contains(string(output), "cannot use") {
				t.Fatalf("wrong consumer unexpectedly compiled or failed for another reason: %v\n%s", err, output)
			}
		})
	}
}
