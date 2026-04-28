class Filelens < Formula
  desc "Inspect and normalize messy data files into clean Parquet tables"
  homepage "https://github.com/kraftaa/filelens"
  url "https://github.com/kraftaa/filelens/archive/refs/tags/v0.1.2.tar.gz"
  sha256 "60b994a95ec5fdb667d7a428dbbec6ac69529f79f6f9eb107251f36cb040edc3"

  depends_on "rust" => :build

  def install
    system "cargo", "install", *std_cargo_args(path: "src")
  end

  test do
    (testpath/"sample.csv").write <<~CSV
      a,b
      1,2
    CSV
    assert_match "Detected:", shell_output("#{bin}/filelens inspect #{testpath}/sample.csv")
  end
end
