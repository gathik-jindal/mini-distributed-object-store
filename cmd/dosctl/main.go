// Command dosctl is the command line client for the object store. It only
// talks to the controller (DOS_URL, default http://localhost:8000).
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/gathik-jindal/mini-distributed-object-store/internal/api"
	"github.com/gathik-jindal/mini-distributed-object-store/internal/client"
)

var (
	baseURL string
	asJSON  bool
)

func main() {
	root := &cobra.Command{
		Use:           "dosctl",
		Short:         "Client for the mini distributed object store",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	def := os.Getenv("DOS_URL")
	if def == "" {
		def = "http://localhost:8000"
	}
	root.PersistentFlags().StringVar(&baseURL, "url", def, "controller URL (env DOS_URL)")
	root.PersistentFlags().BoolVar(&asJSON, "json", false, "print raw JSON")

	root.AddCommand(uploadCmd(), downloadCmd(), statCmd(), lsCmd(), rmCmd(), metaCmd(),
		clusterCmd(), replicationCmd(), repairCmd(), scrubCmd())

	if err := root.Execute(); err != nil {
		var ae *client.APIError
		if errors.As(err, &ae) {
			fmt.Fprintf(os.Stderr, "error %d %s: %s\n", ae.Status, ae.Code, ae.Message)
			if len(ae.Details) > 0 {
				b, _ := json.MarshalIndent(ae.Details, "", "  ")
				fmt.Fprintln(os.Stderr, string(b))
			}
		} else {
			fmt.Fprintln(os.Stderr, "error:", err)
		}
		var ee exitErr
		if errors.As(err, &ee) {
			os.Exit(int(ee))
		}
		os.Exit(1)
	}
}

type exitErr int

func (e exitErr) Error() string { return fmt.Sprintf("exit %d", int(e)) }

func cli() *client.Client { return client.New(baseURL) }

func printJSON(v any) {
	b, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(b))
}

func table(header ...string) *tabwriter.Writer {
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, strings.Join(header, "\t"))
	return tw
}

func human(n int64) string {
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	f := float64(n)
	i := 0
	for f >= 1024 && i < len(units)-1 {
		f /= 1024
		i++
	}
	if i == 0 {
		return fmt.Sprintf("%d B", n)
	}
	return fmt.Sprintf("%.1f %s", f, units[i])
}

func parseKV(pairs []string) (map[string]any, error) {
	out := map[string]any{}
	for _, p := range pairs {
		k, v, ok := strings.Cut(p, "=")
		if !ok || k == "" {
			return nil, fmt.Errorf("expected key=value, got %q", p)
		}
		out[k] = v
	}
	return out, nil
}

func replicas(rs []api.ReplicaInfo) string {
	parts := make([]string, 0, len(rs))
	for _, r := range rs {
		parts = append(parts, fmt.Sprintf("%s:%s/%s", r.WorkerID, r.State, r.WorkerStatus))
	}
	return strings.Join(parts, "  ")
}

func short(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}

// ------------------------------------------------------------------------ objects

func uploadCmd() *cobra.Command {
	var meta []string
	var metaJSON, name, contentType string
	cmd := &cobra.Command{
		Use:   "upload FILE",
		Short: "Upload a file; prints the Object ID and chunk placement",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			custom := map[string]any{}
			if metaJSON != "" {
				if err := json.Unmarshal([]byte(metaJSON), &custom); err != nil {
					return fmt.Errorf("--meta-json: %w", err)
				}
			}
			kv, err := parseKV(meta)
			if err != nil {
				return err
			}
			for k, v := range kv {
				custom[k] = v
			}
			f, err := os.Open(args[0])
			if err != nil {
				return err
			}
			defer f.Close()
			if name == "" {
				name = filepath.Base(args[0])
			}
			res, err := cli().Upload(context.Background(), name, f, client.UploadOptions{ContentType: contentType, Metadata: custom})
			if err != nil {
				return err
			}
			if asJSON {
				printJSON(res)
				return nil
			}
			fmt.Printf("uploaded %s  %s  %d chunk(s)\n", res.Name, human(res.Size), res.ChunkCount)
			fmt.Printf("object_id: %s\n", res.ObjectID)
			fmt.Printf("sha256:    %s\n", res.Checksum)
			if len(res.Chunks) > 0 {
				tw := table("CHUNK", "SIZE", "WORKERS")
				for _, ch := range res.Chunks {
					fmt.Fprintf(tw, "%d\t%s\t%s\n", ch.Index, human(ch.Size), strings.Join(ch.Workers, ", "))
				}
				tw.Flush()
			}
			return nil
		},
	}
	cmd.Flags().StringArrayVarP(&meta, "meta", "m", nil, "custom metadata key=value (repeatable)")
	cmd.Flags().StringVar(&metaJSON, "meta-json", "", "custom metadata as a JSON object")
	cmd.Flags().StringVar(&name, "name", "", "object name (default: file name)")
	cmd.Flags().StringVar(&contentType, "content-type", "", "content type (default: guessed from the name)")
	return cmd
}

func downloadCmd() *cobra.Command {
	var output string
	var noVerify bool
	cmd := &cobra.Command{
		Use:   "download OBJECT_ID",
		Short: "Download an object and verify its sha256 end to end",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			c := cli()
			if output == "" {
				m, err := c.Metadata(ctx, args[0])
				if err != nil {
					return err
				}
				output = filepath.Base(m.Name)
			}
			f, err := os.Create(output)
			if err != nil {
				return err
			}
			h := sha256.New()
			info, derr := c.Download(ctx, args[0], io.MultiWriter(f, h))
			f.Close()
			if derr != nil {
				var ae *client.APIError
				if errors.As(derr, &ae) {
					os.Remove(output)
					return derr
				}
				fmt.Fprintln(os.Stderr, "TRUNCATED:", derr)
				return exitErr(2)
			}
			fmt.Printf("saved %s (%s)\n", output, human(info.Bytes))
			if noVerify {
				return nil
			}
			got := hex.EncodeToString(h.Sum(nil))
			if got != info.ETag {
				fmt.Fprintf(os.Stderr, "sha256 %s != %s  MISMATCH\n", got, info.ETag)
				return exitErr(3)
			}
			fmt.Printf("sha256 %s  MATCH\n", got)
			return nil
		},
	}
	cmd.Flags().StringVarP(&output, "output", "o", "", "file to write (default: object name)")
	cmd.Flags().BoolVar(&noVerify, "no-verify", false, "skip the end-to-end checksum check")
	return cmd
}

func statCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "stat OBJECT_ID",
		Short: "Show system + custom metadata and where every chunk replica lives",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			m, err := cli().Metadata(context.Background(), args[0])
			if err != nil {
				return err
			}
			if asJSON {
				printJSON(m)
				return nil
			}
			cm, _ := json.Marshal(m.CustomMetadata)
			tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
			for _, kv := range [][2]string{
				{"object_id", m.ObjectID}, {"name", m.Name}, {"size", fmt.Sprintf("%d (%s)", m.Size, human(m.Size))},
				{"content_type", m.ContentType}, {"checksum", m.Checksum}, {"chunk_size", fmt.Sprint(m.ChunkSize)},
				{"chunk_count", fmt.Sprint(m.ChunkCount)}, {"created_at", m.CreatedAt}, {"last_modified", m.LastModified},
				{"custom_metadata", string(cm)},
			} {
				fmt.Fprintf(tw, "%s\t%s\n", kv[0], kv[1])
			}
			tw.Flush()
			if len(m.Chunks) > 0 {
				fmt.Println()
				tw = table("#", "CHUNK_ID", "SIZE", "SHA256", "REPLICAS (worker:state/worker_status)")
				for _, ch := range m.Chunks {
					fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\n", ch.Index, short(ch.ChunkID, 14), human(ch.Size),
						ch.Checksum[:12], replicas(ch.Replicas))
				}
				tw.Flush()
			}
			return nil
		},
	}
}

func lsCmd() *cobra.Command {
	var limit, offset int
	cmd := &cobra.Command{
		Use:   "ls",
		Short: "List stored objects",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			l, err := cli().List(context.Background(), limit, offset)
			if err != nil {
				return err
			}
			if asJSON {
				printJSON(l)
				return nil
			}
			fmt.Printf("objects %d-%d of %d\n", offset+min(1, len(l.Items)), offset+len(l.Items), l.Total)
			tw := table("OBJECT_ID", "NAME", "SIZE", "CONTENT_TYPE", "CREATED_AT")
			for _, o := range l.Items {
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", o.ObjectID, o.Name, human(o.Size), o.ContentType, o.CreatedAt)
			}
			return tw.Flush()
		},
	}
	cmd.Flags().IntVar(&limit, "limit", 50, "page size (1-1000)")
	cmd.Flags().IntVar(&offset, "offset", 0, "items to skip")
	return cmd
}

func rmCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "rm OBJECT_ID...",
		Short: "Delete one or more objects",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			for _, id := range args {
				if err := cli().Delete(context.Background(), id); err != nil {
					return err
				}
				fmt.Println("deleted", id)
			}
			return nil
		},
	}
}

func metaCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "meta", Short: "Read or change custom metadata (no re-upload)"}
	show := func(m *api.ObjectMeta) { printJSON(m.CustomMetadata) }
	cmd.AddCommand(
		&cobra.Command{
			Use: "get OBJECT_ID", Short: "Print custom metadata", Args: cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				m, err := cli().Metadata(context.Background(), args[0])
				if err == nil {
					show(m)
				}
				return err
			},
		},
		&cobra.Command{
			Use: "set OBJECT_ID key=value...", Short: "Add or overwrite keys", Args: cobra.MinimumNArgs(2),
			RunE: func(cmd *cobra.Command, args []string) error {
				kv, err := parseKV(args[1:])
				if err != nil {
					return err
				}
				m, err := cli().PatchMetadata(context.Background(), args[0], kv)
				if err == nil {
					show(m)
				}
				return err
			},
		},
		&cobra.Command{
			Use: "rm OBJECT_ID key...", Short: "Remove keys", Args: cobra.MinimumNArgs(2),
			RunE: func(cmd *cobra.Command, args []string) error {
				patch := map[string]any{}
				for _, k := range args[1:] {
					patch[k] = nil
				}
				m, err := cli().PatchMetadata(context.Background(), args[0], patch)
				if err == nil {
					show(m)
				}
				return err
			},
		},
	)
	return cmd
}

// ------------------------------------------------------------------------ cluster

func clusterCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "cluster",
		Short: "Show storage workers and their health",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			w, err := cli().Workers(context.Background())
			if err != nil {
				return err
			}
			if asJSON {
				printJSON(w)
				return nil
			}
			fmt.Printf("%d alive, RF=%d\n", w.Alive, w.ReplicationFactor)
			tw := table("WORKER", "STATUS", "LAST HEARTBEAT", "CHUNKS ON DISK", "REPLICAS OK", "USED", "FREE")
			for _, x := range w.Workers {
				ago := "-"
				if x.SecondsSinceHeartbeat != nil {
					ago = fmt.Sprintf("%.1fs ago", *x.SecondsSinceHeartbeat)
				}
				fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%d\t%s\t%s\n", x.WorkerID, x.Status, ago, x.ChunkCount, x.ReplicasOK,
					human(x.UsedBytes), human(x.FreeBytes))
			}
			return tw.Flush()
		},
	}
}

func printReport(r *api.ReplicationReport) {
	fmt.Printf("replication (RF=%d)\n", r.ReplicationFactor)
	tw := table("TOTAL", "HEALTHY", "UNDER-REPLICATED", "OVER-REPLICATED", "CORRUPT", "UNAVAILABLE", "PENDING DELETES")
	fmt.Fprintf(tw, "%d\t%d\t%d\t%d\t%d\t%d\t%d\n", r.TotalChunks, r.Healthy, r.UnderReplicated, r.OverReplicated,
		r.Corrupt, r.Unavailable, r.PendingDeletes)
	tw.Flush()
	if len(r.ProblemChunks) > 0 {
		fmt.Println("\nproblem chunks")
		tw = table("CHUNK_ID", "LIVE OK", "REPLICAS")
		for i, c := range r.ProblemChunks {
			if i == 30 {
				fmt.Fprintf(tw, "... and %d more\t\t\n", len(r.ProblemChunks)-30)
				break
			}
			fmt.Fprintf(tw, "%s\t%d\t%s\n", short(c.ChunkID, 20), c.LiveOKReplicas, replicas(c.Replicas))
		}
		tw.Flush()
	}
}

func replicationCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "replication",
		Short: "Replication health report",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			r, err := cli().Replication(context.Background(), true)
			if err != nil {
				return err
			}
			if asJSON {
				printJSON(r)
			} else {
				printReport(r)
			}
			return nil
		},
	}
}

func repairCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "repair",
		Short: "Force a repair pass now (it also runs every few seconds)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			r, err := cli().Repair(context.Background())
			if err != nil {
				return err
			}
			if asJSON {
				printJSON(r)
				return nil
			}
			printJSON(r.Summary)
			printReport(&r.Report)
			return nil
		},
	}
}

func scrubCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "scrub",
		Short: "Make every worker re-hash its chunks now; corrupt copies get repaired",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			r, err := cli().Scrub(context.Background())
			if err != nil {
				return err
			}
			if asJSON {
				printJSON(r)
				return nil
			}
			ids := make([]string, 0, len(r.Workers))
			for id := range r.Workers {
				ids = append(ids, id)
			}
			sort.Strings(ids)
			tw := table("WORKER", "SCANNED", "CORRUPT", "MS")
			for _, id := range ids {
				x := r.Workers[id]
				if x.Error != "" {
					fmt.Fprintf(tw, "%s\t-\t%s\t-\n", id, x.Error)
					continue
				}
				bad := "0"
				if len(x.Corrupt) > 0 {
					s := make([]string, len(x.Corrupt))
					for i, c := range x.Corrupt {
						s[i] = short(c, 14)
					}
					bad = strings.Join(s, ", ")
				}
				fmt.Fprintf(tw, "%s\t%d\t%s\t%d\n", id, x.Scanned, bad, x.DurationMS)
			}
			tw.Flush()
			fmt.Printf("newly flagged corrupt replicas: %d\n", r.NewlyFlagged)
			if r.Repair != nil {
				printJSON(r.Repair)
			}
			return nil
		},
	}
}
