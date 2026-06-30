#!/usr/bin/env python3
"""
Dockerfile Matrix Generator
Generates Dockerfiles for all OS × JVM combinations from matrix.yaml
"""

import os
import yaml
from jinja2 import Template
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
MATRIX_FILE = SCRIPT_DIR / "matrix.yaml"
TEMPLATE_FILE = SCRIPT_DIR / "templates" / "kafka.dockerfile.j2"
OUTPUT_DIR = SCRIPT_DIR / "generated"

def load_matrix():
    """Load matrix configuration"""
    with open(MATRIX_FILE) as f:
        return yaml.safe_load(f)

def load_template():
    """Load Jinja2 template"""
    with open(TEMPLATE_FILE) as f:
        return Template(f.read())

def generate_dockerfiles():
    """Generate all Dockerfile combinations"""
    matrix = load_matrix()
    template = load_template()

    kafka_config = matrix['kafka']

    print(f"🏗️  Generating Dockerfiles for Kafka {kafka_config['version']}")
    print()

    for combo in matrix['combinations']:
        name = combo['name']
        output_path = OUTPUT_DIR / name
        output_path.mkdir(parents=True, exist_ok=True)

        # Render template
        context = {
            'name': name,
            'base_image': combo['base_image'],
            'jvm': combo['jvm'],
            'package_manager': combo['package_manager'],
            'kafka': kafka_config
        }

        dockerfile_content = template.render(**context)

        # Write Dockerfile
        dockerfile_path = output_path / "Dockerfile"
        with open(dockerfile_path, 'w') as f:
            f.write(dockerfile_content)

        # Generate build script
        build_script = f"""#!/bin/bash
# Build script for {name}

set -e

IMAGE_NAME="kafka-{name}:{kafka_config['version']}"

echo "🐳 Building $IMAGE_NAME..."
podman build -t "$IMAGE_NAME" .

echo "✅ Built: $IMAGE_NAME"
podman images "$IMAGE_NAME" --format "Size: {{{{.Size}}}}"
"""

        build_script_path = output_path / "build.sh"
        with open(build_script_path, 'w') as f:
            f.write(build_script)
        os.chmod(build_script_path, 0o755)

        print(f"✓ {name}")
        print(f"  Dockerfile: {dockerfile_path}")
        print(f"  Build:      {build_script_path}")
        print()

    print(f"📦 Generated {len(matrix['combinations'])} combinations")
    print()
    print("To build all:")
    print("  cd generated/<name> && ./build.sh")
    print()
    print("To build all at once:")
    print("  for dir in generated/*/; do (cd \"$dir\" && ./build.sh); done")

if __name__ == "__main__":
    generate_dockerfiles()
