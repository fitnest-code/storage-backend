package storage

import (
	"bytes"
	"errors"
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"os"
	"path/filepath"
	"strings"

	"github.com/disintegration/imaging"
	_ "golang.org/x/image/webp"
)

const (
	MaxImageBytes      = 20 * 1024 * 1024
	maxImageDimension  = 2560
	jpegQuality        = 82
	minBytesToCompress = 400 * 1024
)

var (
	ErrFileTooLarge = errors.New("error.file_too_large")
	ErrInvalidType  = errors.New("error.invalid_file_type")
)

// PrepareImage validates an upload as an image (max 20MB, jpeg/png/webp/gif/svg)
// and compresses rasters before Azure. When didCompress is true, outPath is a
// temp file the caller must delete.
func PrepareImage(srcPath string) (outPath, outName string, size int64, didCompress bool, err error) {
	fi, err := os.Stat(srcPath)
	if err != nil {
		return "", "", 0, false, fmt.Errorf("cannot stat upload file: %w", err)
	}
	origName := filepath.Base(srcPath)
	origSize := fi.Size()
	if origSize <= 0 {
		return "", "", 0, false, ErrInvalidType
	}
	if origSize > MaxImageBytes {
		return "", "", 0, false, ErrFileTooLarge
	}

	head, err := readHead(srcPath, 1024)
	if err != nil {
		return "", "", 0, false, ErrInvalidType
	}
	if isSVG(head) {
		return srcPath, origName, origSize, false, nil
	}

	img, openErr := imaging.Open(srcPath, imaging.AutoOrientation(true))
	if openErr != nil {
		return "", "", 0, false, ErrInvalidType
	}

	outPath, outName, size, didCompress = compressDecoded(srcPath, origName, origSize, img)
	return outPath, outName, size, didCompress, nil
}

func compressDecoded(srcPath, origName string, origSize int64, img image.Image) (string, string, int64, bool) {
	if origSize < minBytesToCompress {
		return srcPath, origName, origSize, false
	}

	w := img.Bounds().Dx()
	h := img.Bounds().Dy()
	if w > maxImageDimension || h > maxImageDimension {
		img = imaging.Fit(img, maxImageDimension, maxImageDimension, imaging.Lanczos)
	}

	tmp, err := os.CreateTemp(filepath.Dir(srcPath), "img-compress-*.tmp")
	if err != nil {
		return srcPath, origName, origSize, false
	}
	tmpName := tmp.Name()

	usePNG := !isOpaque(img)
	var outExt string
	if usePNG {
		outExt = ".png"
		err = png.Encode(tmp, img)
	} else {
		outExt = ".jpg"
		err = jpeg.Encode(tmp, img, &jpeg.Options{Quality: jpegQuality})
	}
	_ = tmp.Close()
	if err != nil {
		_ = os.Remove(tmpName)
		return srcPath, origName, origSize, false
	}

	st, err := os.Stat(tmpName)
	if err != nil || st.Size() == 0 || st.Size() >= origSize {
		_ = os.Remove(tmpName)
		return srcPath, origName, origSize, false
	}

	outName := withExtension(origName, outExt)
	fmt.Printf("[Storage] Compressed %s: %d -> %d bytes\n", origName, origSize, st.Size())
	return tmpName, outName, st.Size(), true
}

func readHead(path string, n int) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf := make([]byte, n)
	got, err := f.Read(buf)
	if err != nil && got == 0 {
		return nil, err
	}
	return buf[:got], nil
}

func isSVG(head []byte) bool {
	trimmed := bytes.TrimSpace(head)
	lower := strings.ToLower(string(trimmed))
	return strings.Contains(lower, "<svg")
}

func isOpaque(img image.Image) bool {
	if o, ok := img.(interface{ Opaque() bool }); ok {
		return o.Opaque()
	}
	return true
}

func withExtension(filename, ext string) string {
	if filename == "" {
		return "image" + ext
	}
	dot := strings.LastIndex(filename, ".")
	if dot <= 0 {
		return filename + ext
	}
	return filename[:dot] + ext
}
