// Command mktorrent creates a BitTorrent v1 .torrent for a directory tree,
// for the live test harness. It prints the info-hash (hex) to stdout.
package main

import (
	"crypto/sha1"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func main() {
	root := flag.String("root", "", "directory whose contents form the torrent (torrent name = its basename)")
	out := flag.String("out", "", "output .torrent path")
	announce := flag.String("announce", "", "optional tracker announce URL")
	pieceLen := flag.Int64("piece-len", 256<<10, "piece length in bytes")
	flag.Parse()
	if *root == "" || *out == "" {
		fmt.Fprintln(os.Stderr, "usage: mktorrent -root DIR -out FILE [-announce URL] [-piece-len N]")
		os.Exit(2)
	}
	if err := run(*root, *out, *announce, *pieceLen); err != nil {
		fmt.Fprintln(os.Stderr, "mktorrent:", err)
		os.Exit(1)
	}
}

type file struct {
	path   []string // components relative to root
	length int64
	abs    string
}

func run(root, out, announce string, pieceLen int64) error {
	root, err := filepath.Abs(root)
	if err != nil {
		return err
	}

	var files []file
	err = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		files = append(files, file{
			path:   strings.Split(filepath.ToSlash(rel), "/"),
			length: info.Size(),
			abs:    p,
		})
		return nil
	})
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("no files under %s", root)
	}
	sort.Slice(files, func(i, j int) bool {
		return strings.Join(files[i].path, "/") < strings.Join(files[j].path, "/")
	})

	// Hash the concatenated payload piece by piece.
	var pieceHashes []byte
	h := sha1.New()
	inPiece := int64(0)
	for _, f := range files {
		r, err := os.Open(f.abs)
		if err != nil {
			return err
		}
		for {
			n, err := io.CopyN(h, r, pieceLen-inPiece)
			inPiece += n
			if inPiece == pieceLen {
				pieceHashes = h.Sum(pieceHashes)
				h.Reset()
				inPiece = 0
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				r.Close()
				return err
			}
		}
		r.Close()
	}
	if inPiece > 0 {
		pieceHashes = h.Sum(pieceHashes)
	}

	var info strings.Builder
	writeBenc(&info, map[string]any{
		"files": filesList(files),
		"name":  filepath.Base(root),
		// benc keys sorted lexically: files < name < piece length < pieces
		"piece length": pieceLen,
		"pieces":       string(pieceHashes),
	})
	infoBytes := info.String()

	var t strings.Builder
	top := map[string]any{"info": rawBenc(infoBytes)}
	if announce != "" {
		top["announce"] = announce
	}
	writeBenc(&t, top)
	if err := os.WriteFile(out, []byte(t.String()), 0o644); err != nil {
		return err
	}
	fmt.Printf("%x\n", sha1.Sum([]byte(infoBytes)))
	return nil
}

func filesList(files []file) []any {
	out := make([]any, len(files))
	for i, f := range files {
		path := make([]any, len(f.path))
		for j, c := range f.path {
			path[j] = c
		}
		out[i] = map[string]any{"length": f.length, "path": path}
	}
	return out
}

// rawBenc marks a value as already-bencoded.
type rawBenc string

func writeBenc(w *strings.Builder, v any) {
	switch x := v.(type) {
	case rawBenc:
		w.WriteString(string(x))
	case string:
		fmt.Fprintf(w, "%d:%s", len(x), x)
	case int64:
		fmt.Fprintf(w, "i%de", x)
	case int:
		fmt.Fprintf(w, "i%de", x)
	case []any:
		w.WriteByte('l')
		for _, e := range x {
			writeBenc(w, e)
		}
		w.WriteByte('e')
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		w.WriteByte('d')
		for _, k := range keys {
			writeBenc(w, k)
			writeBenc(w, x[k])
		}
		w.WriteByte('e')
	default:
		panic(fmt.Sprintf("bencode: unsupported type %T", v))
	}
}
