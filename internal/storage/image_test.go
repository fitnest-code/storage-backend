package storage

import (
	"errors"
	"image"
	"image/color"
	"image/jpeg"
	"os"
	"path/filepath"
	"testing"
)

func TestPrepareImage_ShrinksNoisyJPEG(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "photo.jpg")

	img := image.NewRGBA(image.Rect(0, 0, 2400, 2400))
	for y := 0; y < 2400; y++ {
		for x := 0; x < 2400; x++ {
			img.Set(x, y, color.RGBA{R: uint8(x * y), G: uint8(x + y), B: uint8(y * 3), A: 255})
		}
	}
	f, err := os.Create(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := jpeg.Encode(f, img, &jpeg.Options{Quality: 100}); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	orig, err := os.Stat(src)
	if err != nil {
		t.Fatal(err)
	}
	if orig.Size() < minBytesToCompress {
		t.Fatalf("test image too small to exercise compressor: %d", orig.Size())
	}

	outPath, outName, size, did, err := PrepareImage(src)
	if err != nil {
		t.Fatal(err)
	}
	if !did {
		t.Fatal("expected compression to run")
	}
	defer os.Remove(outPath)
	if size >= orig.Size() {
		t.Fatalf("compressed size %d is not smaller than original %d", size, orig.Size())
	}
	if filepath.Ext(outName) != ".jpg" {
		t.Fatalf("expected jpeg output, got %s", outName)
	}
}

func TestPrepareImage_RejectsTooLarge(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "huge.jpg")
	if err := os.WriteFile(src, make([]byte, MaxImageBytes+1), 0644); err != nil {
		t.Fatal(err)
	}
	_, _, _, _, err := PrepareImage(src)
	if !errors.Is(err, ErrFileTooLarge) {
		t.Fatalf("got %v, want ErrFileTooLarge", err)
	}
}

func TestPrepareImage_RejectsNonImage(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "notes.txt")
	if err := os.WriteFile(src, []byte("not an image"), 0644); err != nil {
		t.Fatal(err)
	}
	_, _, _, _, err := PrepareImage(src)
	if !errors.Is(err, ErrInvalidType) {
		t.Fatalf("got %v, want ErrInvalidType", err)
	}
}

func TestPrepareImage_AllowsSVG(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "icon.svg")
	svg := []byte(`<?xml version="1.0"?><svg xmlns="http://www.w3.org/2000/svg" width="8" height="8"></svg>`)
	if err := os.WriteFile(src, svg, 0644); err != nil {
		t.Fatal(err)
	}
	outPath, outName, _, did, err := PrepareImage(src)
	if err != nil {
		t.Fatal(err)
	}
	if did {
		t.Fatal("svg should not be raster-compressed")
	}
	if outPath != src || outName != "icon.svg" {
		t.Fatalf("unexpected svg result %s %s", outPath, outName)
	}
}

func TestWithExtension(t *testing.T) {
	if got := withExtension("photo.PNG", ".jpg"); got != "photo.jpg" {
		t.Fatalf("got %s", got)
	}
	if got := withExtension("photo", ".jpg"); got != "photo.jpg" {
		t.Fatalf("got %s", got)
	}
}
