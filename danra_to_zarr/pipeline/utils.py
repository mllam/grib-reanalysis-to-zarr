import importlib.resources
from pathlib import Path

import markdown2
from loguru import logger


def time_to_str(t):
    return t.isoformat().replace(":", "").replace("-", "")


def convert_markdown_to_html(fp_markdown):
    fp_css = importlib.resources.files(__package__).parent / "static" / "github.css"
    assert fp_css.exists(), f"CSS file not found: {fp_css}"

    with open(fp_markdown, "r") as f:
        markdown_text = f.read()

    html = markdown2.markdown(markdown_text, extras=["fenced-code-blocks", "tables"])

    with open(fp_css, "r") as f:
        css = f.read()

    # add html4 header and add CSS to the HTML output
    html = f"""<!DOCTYPE html>
    <html lang="en">
    <head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>README</title>
    </head>
    <body>
    <style>{css}</style>
    {html}
    </body>
    </html>
    """

    # write the HTML to a file
    fp_html = fp_markdown.with_suffix(".html")
    logger.info(f"Writing to {fp_html}")

    with open(fp_html, "w") as f:
        f.write(html)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("markdown_filepath", type=Path)
    args = parser.parse_args()
    convert_markdown_to_html(fp_markdown=args.markdown_filepath)
