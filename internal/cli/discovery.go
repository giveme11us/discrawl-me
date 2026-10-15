package cli

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/giveme11us/discrawl-me/internal/syncer"
)

func (r *runtime) runDiscover(args []string) error {
	fs := flag.NewFlagSet("discover", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	guild := fs.String("guild", "", "")
	parents := fs.String("thread-parents", "", "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if *guild == "" || len(fs.Args()) != 0 {
		return usageErr(fmt.Errorf("discover requires --guild ID; no implicit/all guild or DM discovery"))
	}
	svc, ok := r.syncer.(interface {
		DiscoverChannels(context.Context, string, []string) (syncer.DiscoveryReport, error)
	})
	if !ok {
		return fmt.Errorf("metadata discovery is not supported by this sync service")
	}
	report, err := svc.DiscoverChannels(r.ctx, *guild, csvList(*parents))
	if err != nil {
		return err
	}
	return r.print(report)
}
