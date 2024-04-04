import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()   

project_urls = {
  'Source': 'https://github.com/embzheng/quant_werobot'
}

setuptools.setup(
    name="quant_werobot",
    version="0.1",
    author="embzheng",
    author_email="embzheng@qq.com",
    description="This is a quant server",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/embzheng/quant_werobot",
    packages=setuptools.find_packages(),
    install_requires=['logging','werobot','os','sys','numpy','pandas','sqlalchemy','datetime'],    
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    project_urls = project_urls
)