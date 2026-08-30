class Filelens < Formula
  desc "Inspect and normalize messy data files into clean Parquet tables"
  homepage "https://github.com/kraftaa/filelens"
  url "https://github.com/kraftaa/filelens/archive/refs/tags/v0.1.3.tar.gz"
  sha256 "740e085ab9b98892e1ab1cf023a7273c27c52fd316955c7cf106d249bc9ebae3"

  depends_on "rust" => :build

  def install
    system "cargo", "install", *std_cargo_args
  end

  test do
    (testpath/"order.cxml").write <<~XML
      <?xml version="1.0" encoding="UTF-8"?>
      <cXML payloadID="homebrew-test" timestamp="2026-08-30T12:00:00Z">
        <Request>
          <OrderRequest>
            <OrderRequestHeader orderID="PO-BREW" orderDate="2026-08-30T12:00:00Z" />
            <ItemOut lineNumber="1" quantity="2">
              <ItemID><SupplierPartID>SKU-001</SupplierPartID></ItemID>
              <ItemDetail>
                <UnitPrice><Money currency="USD">12.50</Money></UnitPrice>
                <Description>Homebrew test item</Description>
                <UnitOfMeasure>EA</UnitOfMeasure>
              </ItemDetail>
            </ItemOut>
          </OrderRequest>
        </Request>
      </cXML>
    XML

    output = shell_output("#{bin}/filelens inspect #{testpath}/order.cxml")
    assert_match "Detected:", output
    assert_match "columns:", output
  end
end
