import setuptools

setuptools.setup(
    name="backup.py",
    version="1.0.0",
    author="Albert Hopkins",
    author_email="marduk@letterboxes.org",
    description="My backup script",
    url="https://github.com/enku/backup",
    license="GPLv3",
    py_modules=["backup", "purgebackups"],
    classifiers=[
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Programming Language :: Python :: 3",
    ],
    python_requires=">=3.6",
    entry_points={
        "console_scripts": [
            "backup = backup:main",
            "purgebackups = purgebackups:main",
        ],
    },
)
